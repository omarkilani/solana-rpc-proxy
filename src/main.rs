use bytes::{Buf, Bytes};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{
    body::Incoming as IncomingBody, header, HeaderMap, Method, Request, Response, StatusCode,
};
use log::{debug, error, info};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

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

    info!("Listening on http://{}", addr);

    loop {
        let (stream, _) = listener.accept().await?;

        let config = Arc::clone(&config);

        tokio::task::spawn(async move {
            let service = service_fn(|req| route(Arc::clone(&config), req));

            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service)
                .await
            {
                error!("Failed to serve connection: {err}");
            }
        });
    }
}

fn create_body<A: Into<Bytes>>(chunk: A) -> BoxBody {
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
            let request_headers = req.headers().clone();

            let request_body = req.collect().await?.aggregate();

            let request_json: Value = serde_json::from_reader(request_body.reader())?;

            debug!("JSON-RPC Request: {}", request_json);

            let response_json: Value =
                handle_jsonrpc(config, request_headers, request_json).await?;

            let response_body = serde_json::to_string(&response_json)?;

            debug!("JSON-RPC Response: {response_body}");

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(create_body(response_body))
                .map_err(|e| e.into())
        }
        (&Method::POST, _, Some("application/json")) => Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(create_body("Access Denied"))
            .unwrap()),

        (&Method::POST, _, _) => Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(create_body("Bad Request"))
            .unwrap()),

        (method, _, _) => Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(create_body(format!("Method {} not allowed.", method)))
            .unwrap()),
    }
}

fn get_from_cache(_requests: Vec<&Value>) -> HashMap<String, Value> {
    // TODO
    //cacheable_requests
    //    .iter()
    //    .map(|(i, request)| get_cache_key(request).unwrap_or(""))
    //    .filter(|cache_key| cache_key != &"")
    //    .collect(),
    HashMap::new()
}

fn add_to_cache(_requests: &Vec<(usize, &Value)>, _responses: &HashMap<usize, Value>) {
    // TODO
}

async fn http_post_json(url: &str, payload: &Value, headers: &HeaderMap) -> Result<Value> {
    use hyper::client::conn::http1::handshake;

    let body = serde_json::to_string(payload)?;

    debug!("Send upstream request: {url}, {headers:#?}, {body}");

    let request = headers
        .into_iter()
        .fold(Request::builder(), |builder, (k, v)| builder.header(k, v))
        .method(Method::POST)
        .uri(url)
        .header(header::CONTENT_TYPE, "application/json")
        .body(create_body(body))?;

    let host = request.uri().host().expect("url has no host");

    let port = request.uri().port_u16().unwrap_or(80);

    let stream: TcpStream = TcpStream::connect(format!("{host}:{port}")).await?;

    let (mut sender, connection) = handshake::<TcpStream, BoxBody>(stream).await?;

    tokio::task::spawn(async move {
        if let Err(err) = connection.await {
            error!("Connection error: {err}");
        }
    });

    let response = sender.send_request(request).await?;

    let mut response_body = String::new();

    response
        .collect()
        .await?
        .aggregate()
        .reader()
        .read_to_string(&mut response_body)?;

    debug!("Received upstream response: {response_body}");

    serde_json::from_str(&response_body).map_err(|e| e.into())
}

async fn get_from_upstream(
    config: Arc<Config>,
    headers: &HeaderMap,
    requests: &Vec<(usize, &Value)>,
) -> Result<HashMap<usize, Value>> {
    debug!("Upstream request batch size: {}", requests.len());

    let payload: Value = serde_json::to_value(
        &requests
            .iter()
            .map(|(_, request)| *request)
            .collect::<Vec<&Value>>(),
    )?;

    let json: Value =
        http_post_json(config.solana_rpc_endpoint.as_str(), &payload, headers).await?;

    debug!("Received upstream response: {json}");

    let responses: Vec<Value> = match json {
        Value::Array(arr) => Ok(arr),
        unexpected => Err(format!("Unexpected upstream response: {unexpected}")),
    }?;

    if requests.len() == responses.len() {
        Ok(HashMap::from_iter(
            requests
                .into_iter()
                .zip(responses.into_iter())
                .map(|((i, _), response)| (*i, response)),
        ))
    } else {
        Err(format!(
            "Can't match requests with responses: {} vs {}",
            requests.len(),
            responses.len()
        )
        .into())
    }
}

fn is_cacheable_request(request: &Value) -> bool {
    get_cache_key(request).is_some()
}

fn get_cache_key(request: &Value) -> Option<String> {
    request["method"].as_str().and_then(|method| match method {
        "getTransaction" => {
            let txn_id = request["params"][0].as_str()?;

            let max_supported_txn_version = request["params"][1]["maxSupportedTransactionVersion"]
                .as_u64()
                .unwrap_or(0);

            Some(format!("{method}::{txn_id}::{max_supported_txn_version}"))
        }
        _ => None,
    })
}

async fn handle_jsonrpc(config: Arc<Config>, headers: HeaderMap, json: Value) -> Result<Value> {
    let (requests, is_batch): (Vec<Value>, bool) = match json {
        Value::Array(array) => (array, true),
        value => (vec![value], false),
    };

    let (cacheable_requests, non_cacheable_requests): (Vec<(usize, &Value)>, Vec<(usize, &Value)>) =
        requests
            .iter()
            .enumerate()
            .partition(|(_, request)| is_cacheable_request(request));

    let cached_responses: HashMap<String, Value> = get_from_cache(
        cacheable_requests
            .iter()
            .map(|(_, request)| *request)
            .collect(),
    );

    let outstanding_requests: Vec<(usize, &Value)> = cacheable_requests
        .into_iter()
        .filter(|(_, request)| {
            !cached_responses.contains_key(&get_cache_key(request).unwrap_or(String::new()))
        })
        .chain(non_cacheable_requests.into_iter())
        .collect();

    let upstream_responses: HashMap<usize, Value> =
        get_from_upstream(config, &headers, &outstanding_requests).await?;

    add_to_cache(&outstanding_requests, &upstream_responses);

    let all_responses: Vec<&Value> = requests
        .iter()
        .enumerate()
        .map(|(i, request)| {
            let response: Option<&Value> = if is_cacheable_request(request) {
                cached_responses.get(&get_cache_key(request).unwrap_or(String::new()))
            } else {
                None
            };

            response
                .or(upstream_responses.get(&i))
                .unwrap_or(&Value::Null)
        })
        .collect();

    Ok(if is_batch {
        Value::Array(all_responses.into_iter().cloned().collect())
    } else {
        all_responses
            .into_iter()
            .cloned()
            .next()
            .unwrap_or(Value::Null)
    })
}
