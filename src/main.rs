use reqwest::Error;
use serde::Deserialize;
use serde_aux::prelude::*;
use tokio_postgres::{Client, NoTls};

#[derive(Deserialize, Debug)]
struct Response {
    jsonrpc: String,
    id: i32,
    result: Result,
}

#[derive(Deserialize, Debug)]
struct Result {
    block_id: BlockId,
    block: Block,
}

#[derive(Deserialize, Debug)]
struct BlockId {
    hash: String,
}

#[derive(Deserialize, Debug)]
struct Block {
    header: Header,
}

#[derive(Deserialize, Debug)]
struct Header {
    chain_id: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    height: i64,

    proposer_address: String,
}

struct ProposerToHeight {
    proposer: String,
    height: i64,
}

const OSMOSIS_LOWEST_HEIGHT: i64 = 9558628;

#[tokio::main]
async fn main() {
    let request_url = format!("https://rpc.osmosis.zone/block?height={height}",
                              height = OSMOSIS_LOWEST_HEIGHT);
    println!("{}", request_url);
    let raw_response = reqwest::get(&request_url).await.unwrap();

    let response: Response = raw_response.json().await.unwrap();
    println!("{:?}", response);

    let proposer_to_height = ProposerToHeight {
        proposer: response.result.block.header.proposer_address,
        height: response.result.block.header.height,
    };

    let (client, connection) =
        tokio_postgres::connect("host=localhost port=5432 user=osmosis password=osmosis", NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let rows = client
        .execute("INSERT INTO proposer_to_height(proposer, height) VALUES ($1, $2)",
                 &[&proposer_to_height.proposer, &proposer_to_height.height])
        .await.unwrap();

    println!("{:?}", rows);
}
