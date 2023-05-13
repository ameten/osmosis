use std::thread;
use std::time::Duration;
use reqwest::Client;

use serde::Deserialize;
use serde_aux::prelude::*;
use tokio::{task, time};
use tokio::task::JoinSet;

#[derive(Deserialize, Debug)]
struct BlockResponse {
    result: BlockResult,
}

#[derive(Deserialize, Debug)]
struct BlockResult {
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

#[derive(Deserialize, Debug)]
struct BlockchainResponse {
    result: BlockchainResult,
}

#[derive(Deserialize, Debug)]
struct BlockchainResult {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    last_height: i64,
}

struct ProposerToHeight {
    proposer: String,
    height: i64,
}

const OSMOSIS_LOWEST_HEIGHT: i64 = 9558628;
const INDEXER_INTERVAL_IN_SECONDS: u64 = 30;
const MAXIMUM_NUMBER_OF_PARALLEL_REQUESTS: i64 = 5;

#[derive(Debug)]
enum Error {
    CouldNotCreateHttpClient,
    CouldNotBuildHttpRequest,
    CouldNotGetResponseFromServer,
    CouldNotParseResponseForBlockAtHeight,
    CouldNotParseResponseForBlockchain,
    CouldNotProcessResponsesInParallel,

    CouldNotCreateDatabaseClient,
    CouldNotFindIndexedHeight,
    CouldNotIndexDuplicateHeight,
    InsertedIncorrectNumberOfRows,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let http_client = Client::builder()
        .build()
        .map_err(|_| Error::CouldNotCreateHttpClient)?;

    let database_client = connect_to_database().await?;

    let forever = task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(INDEXER_INTERVAL_IN_SECONDS));

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

/// When we start database and indexer in docker compose, database is not ready and indexer
/// cannot connect to it. We shall do several attempts to connect to database before failing.
async fn connect_to_database() -> Result<tokio_postgres::Client, Error> {
    for _ in 0..10 {
        thread::sleep(Duration::from_secs(2));
        if let Ok(c) = connect_to_database_unsafe().await { return Ok(c) }
    }

    Err(Error::CouldNotCreateDatabaseClient)
}

async fn connect_to_database_unsafe() -> Result<tokio_postgres::Client, Error> {
    let (database_client, database_connection) =
        tokio_postgres::connect("host=db port=5432 user=osmosis password=osmosis", tokio_postgres::NoTls)
            .await.map_err(|_| Error::CouldNotCreateDatabaseClient)?;

    tokio::spawn(async move {
        if let Err(e) = database_connection.await {
            println!("connection error: {e}");
        }
    });

    Ok(database_client)
}

async fn index(http_client: &Client, database_client: &tokio_postgres::Client)
               -> Result<(), Error> {
    let height_to_index: i64 = database_client
        .query("SELECT max(height) FROM proposer_to_height", &[])
        .await
        .map_err(|_| Error::CouldNotFindIndexedHeight)?
        .get(0)
        .map_or_else(|| OSMOSIS_LOWEST_HEIGHT - 1, |r| {
            r.try_get(0).map_or_else(|_| OSMOSIS_LOWEST_HEIGHT - 1, |v| v)
        }) + 1;

    println!("height_to_index: {height_to_index}");

    let last_height = request_last_height(http_client).await?;
    println!("last_height: {last_height}");

    if height_to_index > last_height {
        println!("Nothing to index");
        return Ok(());
    }

    let mut first_height_to_index = height_to_index;

    while first_height_to_index < last_height {
        let last_height_to_index = if first_height_to_index + MAXIMUM_NUMBER_OF_PARALLEL_REQUESTS > last_height {
            last_height
        } else {
            first_height_to_index + MAXIMUM_NUMBER_OF_PARALLEL_REQUESTS
        };

        let proposers_to_height =
            request_proposers(http_client, first_height_to_index, last_height_to_index).await?;

        let query = prepare_statement(&proposers_to_height);
        println!("query: {}", query);

        let count_rows_inserted = database_client
            .execute(&query, &[])
            .await
            .map_err(|_| Error::CouldNotIndexDuplicateHeight)? as usize;

        if count_rows_inserted != proposers_to_height.len() {
            return Err(Error::InsertedIncorrectNumberOfRows);
        }

        first_height_to_index = last_height_to_index;
    }

    Ok(())
}

async fn request_last_height(http_client: &Client) -> Result<i64, Error> {
    let raw_response =
        request(http_client.clone(), "https://rpc.osmosis.zone/blockchain".to_string())
            .await?;
    let response: BlockchainResponse = raw_response.json()
        .await
        .map_err(|_| Error::CouldNotParseResponseForBlockchain)?;
    Ok(response.result.last_height)
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
async fn request_proposers(http_client: &Client,
                           first_height_to_index: i64,
                           last_height_to_index: i64)
                           -> Result<Vec<ProposerToHeight>, Error> {
    let mut set = JoinSet::new();

    for height in first_height_to_index..last_height_to_index {
        let request_url = format!("https://rpc.osmosis.zone/block?height={height}");
        println!("request_url: {}", request_url);

        let future_response = request(http_client.clone(), request_url);
        set.spawn(future_response);
    }

    let mut proposers_to_height = Vec::new();

    while let Some(res) = set.join_next().await {
        let raw_response = res
            .map_err(|_| Error::CouldNotProcessResponsesInParallel)??;

        let response: BlockResponse = raw_response.json()
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

async fn request(http_client: Client, request_url: String)
                 -> Result<reqwest::Response, Error> {
    let request = http_client.get(request_url).build()
        .map_err(|_| Error::CouldNotBuildHttpRequest)?;

    let raw_response = http_client.execute(request)
        .await
        .map_err(|_| Error::CouldNotGetResponseFromServer)?;

    Ok(raw_response)
}
