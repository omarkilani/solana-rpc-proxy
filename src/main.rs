use bytes::{Buf, Bytes};
use dotenvy::dotenv;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{
    body::Incoming as IncomingBody, header, HeaderMap, Method, Request, Response, StatusCode,
};
use log::{debug, error, info};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgPoolOptions, PgQueryResult};
use sqlx::{PgPool, QueryBuilder};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

const RPC_VERSION: &str = "2.0";

#[derive(Deserialize, Debug)]
struct Config {
    database_url: String,
    listen_address: SocketAddr,
    rpc_token: String,
    solana_rpc_endpoint: String,
}

struct Env {
    config: Config,
    db_pool: PgPool,
    http_client: Client,
}

#[derive(Debug, Deserialize, Serialize)]
enum RpcRequest {
    Batch(Vec<RpcCall>),
    Single(RpcCall),
}

impl RpcRequest {
    fn new(calls: Vec<RpcCall>) -> Option<RpcRequest> {
        if calls.len() > 1 {
            Some(RpcRequest::Batch(calls))
        } else {
            calls
                .into_iter()
                .next()
                .map(|call| RpcRequest::Single(call))
        }
    }

    fn calls(self) -> Vec<RpcCall> {
        match self {
            RpcRequest::Batch(calls) => calls.into_iter().collect(),
            RpcRequest::Single(call) => vec![call],
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum RpcResponse {
    Batch(Vec<RpcResult>),
    Single(RpcResult),
}

impl RpcResponse {
    fn error(code: i64, message: &str, data: Option<Value>) -> RpcResponse {
        RpcResponse::Single(RpcResult {
            jsonrpc: RPC_VERSION.to_string(),
            result: None,
            error: Some(RpcError {
                code,
                data,
                message: message.to_string(),
            }),
            id: None,
        })
    }

    fn results(self) -> Vec<RpcResult> {
        match self {
            RpcResponse::Batch(results) => results.into_iter().collect(),
            RpcResponse::Single(result) => vec![result],
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcCall {
    id: Option<Id>,
    jsonrpc: String,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcResult {
    error: Option<RpcError>,
    id: Option<Id>,
    jsonrpc: String,
    result: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
enum Id {
    Null,
    Number(u64),
    String(String),
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcError {
    code: i64,
    data: Option<Value>,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().unwrap_or_default();

    pretty_env_logger::init();

    let config = envy::from_env::<Config>().expect(
        "Please provide DATABASE_URL, LISTEN_ADDRESS, RPC_TOKEN, and SOLANA_RPC_ENDPOINT env vars",
    );

    let db_pool: PgPool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;

    info!("Created a db pool: {:?}", &db_pool);

    let http_client = Client::new();

    info!("Created an http client: {:?}", &http_client);

    let env = Arc::new(Env {
        config,
        db_pool,
        http_client,
    });

    let listener = TcpListener::bind(&env.config.listen_address).await?;

    info!("Listening on http://{}", &env.config.listen_address);

    loop {
        let (stream, _) = listener.accept().await?;

        let env = Arc::clone(&env);

        tokio::task::spawn(async move {
            let service = service_fn(|req| handle_request(Arc::clone(&env), req));

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

async fn handle_request(env: Arc<Env>, req: Request<IncomingBody>) -> Result<Response<BoxBody>> {
    route(env, req).await.or_else(|err: GenericError| {
        debug!("Unexpected error: {err}");

        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(create_body("Internal Server Error"))
            .map_err(|err| err.into())
    })
}

async fn route(env: Arc<Env>, req: Request<IncomingBody>) -> Result<Response<BoxBody>> {
    match (
        req.method(),
        req.uri().path(),
        req.headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().ok())
            .flatten(),
    ) {
        (&Method::POST, path, Some("application/json"))
            if path == format!("/{}/", env.config.rpc_token) =>
        {
            let request_headers = req.headers().clone();

            let request_body = req.collect().await?.aggregate();

            let rpc_response =
                match serde_json::from_reader::<_, RpcRequest>(request_body.reader()) {
                    Ok(rpc_request) => {
                        debug!("JSON-RPC Request: {rpc_request:?}");

                        handle_jsonrpc(env, request_headers, rpc_request).await
                    }
                    Err(err) => {
                        debug!("JSON-RPC Invalid request: {err:?}");

                        Ok(RpcResponse::error(-32600, "Invalid Request", None))
                    }
                }
                .unwrap_or(RpcResponse::error(-32603, "Internal error", None));

            let response_body = serde_json::to_string(&rpc_response)?;

            debug!("JSON-RPC Response: {response_body}");

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(create_body(response_body))
        }
        (&Method::POST, _, Some("application/json")) => Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(create_body("Access Denied")),

        (&Method::POST, _, _) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(create_body("Bad Request")),

        (method, _, _) => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(create_body(format!("Method {method} not allowed."))),
    }
    .map_err(|err| err.into())
}

#[derive(sqlx::FromRow)]
struct CachedRpcResult {
    key: String,
    response: String,
}

async fn get_from_cache(env: Arc<Env>, calls: &Vec<RpcCall>) -> HashMap<CacheKey, RpcResult> {
    let keys: Vec<String> = calls
        .into_iter()
        .filter_map(|call| get_cache_key(call))
        .map(|CacheKey(string_value)| string_value)
        .collect();

    sqlx::query_as::<_, CachedRpcResult>("SELECT * FROM cached_response WHERE key = ANY($1)")
        .bind(keys)
        .fetch_all(&env.db_pool)
        .await
        .map_or(HashMap::new(), |cached_responses| {
            cached_responses
                .into_iter()
                .filter_map(|CachedRpcResult { key, response }| {
                    serde_json::from_str(&response)
                        .ok()
                        .map(|rpc_result| (CacheKey(key), rpc_result))
                })
                .collect()
        })
}

async fn add_to_cache(
    env: Arc<Env>,
    calls: &Vec<RpcCall>,
    results: &Vec<RpcResult>,
) -> Result<PgQueryResult> {
    let cacheable_results: Vec<(CacheKey, Value)> = calls
        .into_iter()
        .filter_map(|call| {
            let cache_key = get_cache_key(call)?;
            let result = results.into_iter().find(|result| result.id == call.id)?;
            let json_value = serde_json::to_value(result).ok()?;

            Some((cache_key, json_value))
        })
        .collect();

    if cacheable_results.is_empty() {
        Ok(PgQueryResult::default())
    } else {
        QueryBuilder::new("INSERT INTO cached_response(key, response) ")
            .push_values(
                cacheable_results,
                |mut builder, (CacheKey(key), json_value)| {
                    builder.push_bind(key).push_bind(json_value);
                },
            )
            .build()
            .execute(&env.db_pool)
            .await
            .map_err(|err| err.into())
    }
}

async fn http_post_json(
    env: Arc<Env>,
    url: &str,
    payload: &Value,
    headers: HeaderMap,
) -> Result<Value> {
    debug!("HTTP request body: {payload}");

    let body = serde_json::to_string(payload)?;

    let excluded_headers = ["host", "content-length"];

    let request_builder = headers
        .iter()
        .filter(|(k, _)| !excluded_headers.contains(&k.as_str().to_lowercase().as_str()))
        .fold(env.http_client.post(url), |builder, (k, v)| {
            builder.header(k, v)
        })
        .timeout(Duration::from_secs(30))
        .body(body);

    debug!("HTTP request: {request_builder:?}");

    let response = request_builder.send().await?;

    debug!("HTTP response: {response:?}");

    let text = response.text().await?;

    debug!("HTTP response body: {text}");

    serde_json::from_str(&text).map_err(|e| e.into())
}

async fn get_from_upstream(
    env: Arc<Env>,
    headers: HeaderMap,
    request: &RpcRequest,
) -> Result<RpcResponse> {
    let payload: Value = serde_json::to_value(request)?;

    let json: Value = http_post_json(
        Arc::clone(&env),
        env.config.solana_rpc_endpoint.as_str(),
        &payload,
        headers,
    )
    .await?;

    serde_json::from_value(json).map_err(|err| err.into())
}

fn is_cacheable_call(call: &RpcCall) -> bool {
    get_cache_key(call).is_some()
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct CacheKey(String);

fn get_cache_key(call: &RpcCall) -> Option<CacheKey> {
    match call.method.as_str() {
        "getTransaction" => {
            let txn_id = call.params.as_ref().and_then(|value| value[0].as_str())?;

            let max_supported_txn_version = call
                .params
                .as_ref()
                .and_then(|value| value[1]["maxSupportedTransactionVersion"].as_i64())
                .unwrap_or(-1);

            Some(CacheKey(format!(
                "getTransaction::{txn_id}::{max_supported_txn_version}"
            )))
        }
        _ => None,
    }
}

async fn handle_jsonrpc(
    env: Arc<Env>,
    headers: HeaderMap,
    request: RpcRequest,
) -> Result<RpcResponse> {
    let (cacheable_calls, non_cacheable_calls): (Vec<RpcCall>, Vec<RpcCall>) = request
        .calls()
        .into_iter()
        .partition(|call| is_cacheable_call(call));

    let cached_results: HashMap<CacheKey, RpcResult> =
        get_from_cache(Arc::clone(&env), &cacheable_calls).await;

    let outstanding_calls: Vec<RpcCall> = cacheable_calls
        .into_iter()
        .filter(|call| {
            get_cache_key(call)
                .map(|cache_key| !cached_results.contains_key(&cache_key))
                .unwrap_or(true)
        })
        .chain(non_cacheable_calls.into_iter())
        .collect();

    let upstream_request = RpcRequest::new(outstanding_calls);

    let upstream_results: Vec<RpcResult> = match upstream_request {
        Some(upstream_request) => get_from_upstream(Arc::clone(&env), headers, &upstream_request)
            .await
            .map(|upstream_response| upstream_response.results())?,
        _ => vec![],
    };

    add_to_cache(Arc::clone(&env), upstream_request.calls(), &upstream_results).await?;

    todo!();
}

//async fn handle_jsonrpc_old(
//    env: Arc<Env>,
//    headers: HeaderMap,
//    json: Value,
//) -> Result<Value> {
//    let (requests, is_batch): (Vec<Value>, bool) = match json {
//        Value::Array(array) => (array, true),
//        value => (vec![value], false),
//    };
//
//    let (cacheable_requests, non_cacheable_requests): (Vec<(usize, &Value)>, Vec<(usize, &Value)>) =
//        requests
//            .iter()
//            .enumerate()
//            .partition(|(_, request)| is_cacheable_request(request));
//
//    let cached_responses: HashMap<String, Value> = get_from_cache(
//        Arc::clone(&env),
//        cacheable_requests
//            .iter()
//            .map(|(_, request)| *request)
//            .collect(),
//    )
//    .await;
//
//    let outstanding_requests: Vec<(usize, &Value)> = cacheable_requests
//        .into_iter()
//        .filter(|(_, request)| {
//            !cached_responses.contains_key(&get_cache_key(request).unwrap_or(String::new()))
//        })
//        .chain(non_cacheable_requests.into_iter())
//        .collect();
//
//    let upstream_responses: HashMap<usize, Value> = if outstanding_requests.is_empty() {
//        HashMap::new()
//    } else {
//        get_from_upstream(Arc::clone(&env), headers, &outstanding_requests).await?
//    };
//
//    if !upstream_responses.is_empty() {
//        add_to_cache(Arc::clone(&env), &outstanding_requests, &upstream_responses).await?;
//    }
//
//    let all_responses: Vec<&Value> = requests
//        .iter()
//        .enumerate()
//        .map(|(i, request)| {
//            let response: Option<&Value> = if is_cacheable_request(request) {
//                cached_responses.get(&get_cache_key(request).unwrap_or(String::new()))
//            } else {
//                None
//            };
//
//            response
//                .or(upstream_responses.get(&i))
//                .unwrap_or(&Value::Null)
//        })
//        .collect();
//
//    info!(
//        "JSON-RPC Response batch size: {} (from_cache={}, from_upstream={})",
//        all_responses.len(),
//        cached_responses.len(),
//        upstream_responses.len()
//    );
//
//    Ok(if is_batch {
//        Value::Array(all_responses.into_iter().cloned().collect())
//    } else {
//        all_responses
//            .into_iter()
//            .cloned()
//            .next()
//            .unwrap_or(Value::Null)
//    })
//}
