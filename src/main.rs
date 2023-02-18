use bytes::{Buf, Bytes};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{
    body::Incoming as IncomingBody, header, HeaderMap, Method, Request, Response, StatusCode,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

async fn route(config: Arc<Config>, req: Request<IncomingBody>) -> Result<Response<BoxBody>> {
    match (
        req.method(),
        req.uri().path(),
        req.headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().ok())
            .flatten(),
    ) {
        (&Method::POST, path, Some("application/json"))
            if path == format!("/{}/", config.rpc_token) =>
        {
            handle(config, req).await
        }
        (&Method::POST, _, Some("application/json")) => Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(full("Access Denied"))
            .unwrap()),

        (&Method::POST, _, _) => Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(full("Bad Request"))
            .unwrap()),

        (method, _, _) => Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(full(format!("Method {} not allowed.", method)))
            .unwrap()),
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    rpc_token: String,
    solana_rpc_endpoint: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let config = Arc::new(
        envy::from_env::<Config>()
            .expect("Please provide RPC_TOKEN and SOLANA_RPC_ENDPOINT env var"),
    );

    let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    let listener = TcpListener::bind(&addr).await?;

    println!("Listening on http://{}", addr);

    loop {
        let (stream, _) = listener.accept().await?;

        let config = Arc::clone(&config);

        tokio::task::spawn(async move {
            let service = service_fn(|req| route(Arc::clone(&config), req));

            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service)
                .await
            {
                println!("Failed to serve connection: {:?}", err);
            }
        });
    }
}

fn get_txns_from_cache(txn_ids: Vec<&str>) -> HashMap<&str, Value> {
    todo!();
}

fn add_to_cache(requests: &Vec<(usize, &Value)>, responses: &HashMap<usize, Value>) {
    todo!();
}

fn get_from_upstream(
    config: Arc<Config>,
    headers: HeaderMap,
    requests: &Vec<(usize, &Value)>,
) -> HashMap<usize, Value> {
    //let upstream_responses_by_idx: HashMap<usize, &Value> = outstanding
    //    .iter()
    //    .zip(upstream_responses.iter())
    //    .map(|((i, _), response)| (*i, response))
    //    .collect();
    todo!();
}

fn is_txn_request(request: &Value) -> bool {
    request["method"] == "getTransaction"
}

fn get_txn_id(request: &Value) -> Option<&str> {
    request["params"][0].as_str()
}

async fn handle(
    config: Arc<Config>,
    http_request: Request<IncomingBody>,
) -> Result<Response<BoxBody>> {
    let headers = http_request.headers().clone();

    let http_request_body = http_request.collect().await?.aggregate();

    let http_request_json: Value = serde_json::from_reader(http_request_body.reader())?;

    let (requests, is_batch): (Vec<Value>, bool) = match http_request_json {
        Value::Array(array) => (array, true),
        value => (vec![value], false),
    };

    let (cacheable_requests, non_cacheable_requests): (Vec<(usize, &Value)>, Vec<(usize, &Value)>) =
        requests
            .iter()
            .enumerate()
            .partition(|(i, request)| is_txn_request(request));

    let cached_txns: HashMap<&str, Value> = get_txns_from_cache(
        cacheable_requests
            .iter()
            .map(|(i, request)| get_txn_id(request).unwrap_or(""))
            .filter(|txn_id| txn_id != &"")
            .collect(),
    );

    let outstanding_requests: Vec<(usize, &Value)> = cacheable_requests
        .iter()
        .filter(|(i, request)| !cached_txns.contains_key(get_txn_id(request).unwrap_or("")))
        .chain(non_cacheable_requests.iter())
        .copied()
        .collect();

    let upstream_responses: HashMap<usize, Value> =
        get_from_upstream(config, headers, &outstanding_requests);

    add_to_cache(&outstanding_requests, &upstream_responses);

    let all_responses: Vec<&Value> = requests
        .iter()
        .enumerate()
        .map(|(i, request)| {
            let response: Option<&Value> = if is_txn_request(request) {
                cached_txns.get(get_txn_id(request).unwrap_or(""))
            } else {
                None
            };

            response
                .or(upstream_responses.get(&i))
                .unwrap_or(&Value::Null)
        })
        .collect();

    let http_response_body = serde_json::to_string(
        &(if is_batch {
            Value::Array(all_responses.into_iter().cloned().collect())
        } else {
            all_responses
                .into_iter()
                .cloned()
                .next()
                .unwrap_or(Value::Null)
        }),
    )?;

    let http_response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(full(http_response_body))?;

    Ok(http_response)
}
