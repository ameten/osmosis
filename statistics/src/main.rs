use std::net::SocketAddr;

use axum::{
    http::StatusCode,
    Json,
    response::IntoResponse,
    Router, routing::{get},
};
use axum::extract::{Query, State};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

#[derive(Deserialize, Debug)]
struct Params {
    validator: String,
}

#[derive(Serialize, Debug)]
struct Response {
    heights: Vec<i64>,
}

#[tokio::main]
async fn main() {
    let manager =
        PostgresConnectionManager::new_from_stringlike("host=db user=postgres", NoTls)
            .unwrap();
    let pool = Pool::builder().build(manager).await.unwrap();

    let app = Router::new()
        .route("/stat", get(handler))
        .with_state(pool);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handler(Query(params): Query<Params>, State(pool): State<Pool<PostgresConnectionManager<NoTls>>>)
                 -> impl IntoResponse {
    let validator = params.validator;

    let conn = pool.get().await
        .unwrap();

    let rows = conn
        .query("SELECT height FROM proposer_to_height WHERE proposer = $1", &[&validator])
        .await
        .unwrap();

    let heights: Vec<i64> = rows
        .into_iter()
        .map(|r| r.get(0))
        .collect();

    let response = Response {
        heights
    };

    (StatusCode::OK, Json(response))
}
