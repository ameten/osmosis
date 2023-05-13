use serde::Deserialize;
use reqwest::Error;

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
    height: String,
    proposer_address: String,
}

#[tokio::main]
async fn main() -> core::result::Result<(), Error> {
    let request_url = format!("https://rpc.osmosis.zone/block?height={height}",
                              height = "9558629");
    println!("{}", request_url);
    let raw_response = reqwest::get(&request_url).await?;

    let response: Response = raw_response.json().await?;
    println!("{:?}", response);
    Ok(())
}
