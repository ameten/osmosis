use std::time::Duration;

use serde::Deserialize;
use serde_aux::prelude::*;
use tokio::{task, time};

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

#[derive(Debug)]
enum Error {
    CouldNotCreateHttpClient,
    CouldNotBuildHttpRequest,
    CouldNotGetBlockAtHeight,
    CouldNotParseResponseForBlockAtHeight,

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
    let height_indexed: i64 = database_client
        .query("SELECT max(height) FROM proposer_to_height", &[])
        .await
        .map_err(|_| Error::CouldNotFindIndexedHeight)?
        .get(0)
        .map_or_else(|| OSMOSIS_LOWEST_HEIGHT - 1, |r| r.get(0));

    let height_to_index = height_indexed + 1;
    println!("height_to_index: {height_to_index}");

    let request_url = format!("https://rpc.osmosis.zone/block?height={height_to_index}");
    println!("request_url: {}", request_url);

    let request = http_client.get(&request_url).build()
        .map_err(|_| Error::CouldNotBuildHttpRequest)?;

    let raw_response = http_client.execute(request)
        .await
        .map_err(|_| Error::CouldNotGetBlockAtHeight)?;

    let response: Response = raw_response.json()
        .await
        .map_err(|_| Error::CouldNotParseResponseForBlockAtHeight)?;
    println!("{:?}", response);

    let proposer_to_height = ProposerToHeight {
        proposer: response.result.block.header.proposer_address,
        height: response.result.block.header.height,
    };

    let count_rows_inserted = database_client
        .execute("INSERT INTO proposer_to_height(proposer, height) VALUES ($1, $2)",
                 &[&proposer_to_height.proposer, &proposer_to_height.height])
        .await
        .map_err(|_| Error::CouldNotIndexDuplicateHeight)?;

    println!("{:?}", count_rows_inserted);

    if count_rows_inserted != 1 {
        return Err(Error::InsertedIncorrectNumberOfRows);
    }

    Ok(())
}
