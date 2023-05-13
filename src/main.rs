use std::time::Duration;

use serde::Deserialize;
use serde_aux::prelude::*;
use tokio::{task, time};
use tokio::task::JoinSet;

#[derive(Deserialize, Debug)]
struct Response {
    result: Result,
}

#[derive(Deserialize, Debug)]
struct Result {
    block: Block,
}

#[derive(Deserialize, Debug)]
struct Block {
    header: Header,
}

#[derive(Deserialize, Debug)]
struct Header {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    height: i64,
    proposer_address: String,
}

struct ProposerToHeight {
    proposer: String,
    height: i64,
}

const OSMOSIS_LOWEST_HEIGHT: i64 = 9558628;
const INDEXER_INTERVAL: u64 = 5;
const MAXIMUM_NUMBER_OF_PARALLEL_REQUESTS: i64 = 5;

#[derive(Debug)]
enum Error {
    CouldNotCreateHttpClient,
    CouldNotBuildHttpRequest,
    CouldNotGetResponseFromServer,
    CouldNotParseResponseForBlockAtHeight,
    CouldNotProcessResponsesInParallel,

    CouldNotCreateDatabaseClient,
    CouldNotFindIndexedHeight,
    CouldNotIndexDuplicateHeight,
    InsertedIncorrectNumberOfRows,
}

#[tokio::main]
async fn main() -> core::result::Result<(), Error> {
    let http_client = reqwest::Client::builder()
        .build()
        .map_err(|_| Error::CouldNotCreateHttpClient)?;

    let (database_client, database_connection) =
        tokio_postgres::connect("host=localhost port=5432 user=osmosis password=osmosis", tokio_postgres::NoTls)
            .await.map_err(|_| Error::CouldNotCreateDatabaseClient)?;

    tokio::spawn(async move {
        if let Err(e) = database_connection.await {
            println!("connection error: {e}");
        }
    });

    let forever = task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(INDEXER_INTERVAL));

        loop {
            interval.tick().await;
            index(&http_client, &database_client)
                .await
                .unwrap_or_else(|e| println!("Indexing error {e:?}"));
        }
    });

    forever.await.expect("Recurring task failed");
    Ok(())
}

async fn index(http_client: &reqwest::Client, database_client: &tokio_postgres::Client)
               -> core::result::Result<(), Error> {
    let height_to_index: i64 = database_client
        .query("SELECT max(height) FROM proposer_to_height", &[])
        .await
        .map_err(|_| Error::CouldNotFindIndexedHeight)?
        .get(0)
        .map_or_else(|| OSMOSIS_LOWEST_HEIGHT - 1, |r| r.get(0)) + 1;

    println!("height_to_index: {height_to_index}");

    let proposers_to_height =
        request_proposers(http_client, height_to_index).await?;

    let query = prepare_statement(&proposers_to_height);
    println!("query: {}", query);

    let count_rows_inserted = database_client
        .execute(&query, &[])
        .await
        .map_err(|_| Error::CouldNotIndexDuplicateHeight)? as usize;

    println!("{:?}", count_rows_inserted);

    if count_rows_inserted != proposers_to_height.len() {
        return Err(Error::InsertedIncorrectNumberOfRows);
    }

    Ok(())
}

fn prepare_statement(proposers_to_height: &Vec<ProposerToHeight>) -> String {
    let mut query = "INSERT INTO proposer_to_height(proposer, height) VALUES".to_string();

    for proposer_to_height in proposers_to_height {
        query.push_str(&format!("('{}',{}),", proposer_to_height.proposer, proposer_to_height.height));
    }

    query.remove(query.len() - 1);

    query
}

/// Request information about block at MAXIMUM_NUMBER_OF_PARALLEL_REQUESTS heights in parallel
/// I have not found endpoint which would give block info in bulk
/// https://rpc.osmosis.zone/blockchain?minHeight=9558628 gives only 20 heights back from the top
/// Requests are made in parallel and the number of such requests is limited to avoid overloading
/// the server.
async fn request_proposers(http_client: &reqwest::Client, height_to_index: i64)
                           -> core::result::Result<Vec<ProposerToHeight>, Error> {
    let mut set = JoinSet::new();

    for i in 0..MAXIMUM_NUMBER_OF_PARALLEL_REQUESTS {
        let request_url = format!("https://rpc.osmosis.zone/block?height={}", height_to_index + i);
        println!("request_url: {}", request_url);

        let future_response = request(http_client.clone(), request_url);
        set.spawn(future_response);
    }

    let mut proposers_to_height = Vec::new();

    while let Some(res) = set.join_next().await {
        let raw_response = res
            .map_err(|_| Error::CouldNotProcessResponsesInParallel)??;

        let response: Response = raw_response.json()
            .await
            .map_err(|_| Error::CouldNotParseResponseForBlockAtHeight)?;
        println!("{:?}", response);

        let proposer_to_height = ProposerToHeight {
            proposer: response.result.block.header.proposer_address,
            height: response.result.block.header.height,
        };

        proposers_to_height.push(proposer_to_height);
    }

    Ok(proposers_to_height)
}

async fn request(http_client: reqwest::Client, request_url: String)
                 -> core::result::Result<reqwest::Response, Error> {

    let request = http_client.get(request_url).build()
        .map_err(|_| Error::CouldNotBuildHttpRequest)?;

    let raw_response = http_client.execute(request)
        .await
        .map_err(|_| Error::CouldNotGetResponseFromServer)?;

    Ok(raw_response)
}
