use brotli::enc::backward_references::BrotliEncoderMode;
use brotli::enc::BrotliEncoderParams;
use brotli::BrotliCompress;
use bytes::{Buf, Bytes};
use dotenvy::dotenv;
use flate2::write::{DeflateEncoder, GzEncoder};
use flate2::Compression;
use fly_accept_encoding::Encoding;
use futures::future;
use futures::future::BoxFuture;
use futures::TryFutureExt;
use http::response;
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
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Write;
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
    solana_rpc_endpoints: Vec<String>,
}

struct Env {
    config: Config,
    db_pool: PgPool,
    http_client: Client,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum RpcRequest {
    Batch(Vec<RpcCall>),
    Single(RpcCall),
}

impl RpcRequest {
    fn new(calls: Vec<&RpcCall>) -> Option<RpcRequest> {
        if calls.len() > 1 {
            Some(RpcRequest::Batch(calls.into_iter().cloned().collect()))
        } else {
            calls.into_iter().next().cloned().map(RpcRequest::Single)
        }
    }

    fn calls(&self) -> Vec<&RpcCall> {
        match self {
            RpcRequest::Batch(calls) => calls.iter().collect(),
            RpcRequest::Single(call) => vec![call],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum RpcResponse {
    Batch(Vec<RpcResult>),
    Single(RpcResult),
}

impl RpcResponse {
    fn results(&self) -> Vec<&RpcResult> {
        match self {
            RpcResponse::Batch(results) => results.iter().collect(),
            RpcResponse::Single(result) => vec![result],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RpcCall {
    id: Option<Id>,
    jsonrpc: String,
    method: String,
    params: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RpcResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
    id: Option<Id>,
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
}

impl RpcResult {
    fn error(id: Option<Id>, code: i64, message: &str) -> RpcResult {
        RpcResult {
            jsonrpc: RPC_VERSION.to_string(),
            result: None,
            error: Some(RpcError {
                code,
                data: None,
                message: message.to_string(),
            }),
            id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Hash, Eq)]
#[serde(untagged)]
enum Id {
    Null,
    Number(u64),
    String(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
        "Please provide DATABASE_URL, LISTEN_ADDRESS, RPC_TOKEN, and SOLANA_RPC_ENDPOINTS env vars",
    );

    info!("Config: {:?}", &config);

    let db_pool: PgPool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;

    info!("DB Pool: {:?}", &db_pool);

    let http_client = Client::new();

    info!("HTTP Client: {:?}", &http_client);

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
            let service = service_fn(|req| handle_http_request(Arc::clone(&env), req));

            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service)
                .await
            {
                error!("Failed to serve connection: {err}");
            }
        });
    }
}

fn create_body<A: Into<Bytes>>(chunk: A, encoding: Encoding) -> BoxBody {
    let raw = chunk.into();

    let raw_len = raw.len();

    let encoded: Bytes = match encoding {
        Encoding::Identity => Some(raw),
        Encoding::Zstd => {
            let input: &[u8] = &raw;
            let mut output = Vec::with_capacity(input.len() + 64);

            zstd::stream::copy_encode(input, &mut output, zstd::DEFAULT_COMPRESSION_LEVEL)
                .map(|_| output.into())
                .map_err(|err| error!("Cannot encode body using zstd: {err}"))
                .ok()
        }
        Encoding::Brotli => {
            let mut input: &[u8] = &raw;
            let mut output = Vec::with_capacity(input.len() + 64);
            let params = BrotliEncoderParams {
                mode: BrotliEncoderMode::BROTLI_MODE_TEXT,
                ..Default::default()
            };

            BrotliCompress(&mut input, &mut output, &params)
                .map(|_| output.into())
                .map_err(|err| error!("Cannot encode body using brotli: {err}"))
                .ok()
        }
        Encoding::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

            encoder
                .write_all(&raw)
                .and(encoder.finish())
                .map(|vec| vec.into())
                .map_err(|err| error!("Cannot encode body using gzip: {err}"))
                .ok()
        }
        Encoding::Deflate => {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());

            encoder
                .write_all(&raw)
                .and(encoder.finish())
                .map(|vec| vec.into())
                .map_err(|err| error!("Cannot encode body using deflate: {err}"))
                .ok()
        }
    }
    .unwrap_or(Bytes::new());

    debug!(
        "Body encoding: {encoding:?} ({}bytes -> {}bytes)",
        raw_len,
        encoded.len()
    );

    Full::new(encoded).map_err(|never| match never {}).boxed()
}

fn preferred_encoding(request_headers: &HeaderMap) -> Encoding {
    fly_accept_encoding::encodings_iter(request_headers)
        .filter_map(|result| result.ok())
        .filter_map(|(encoding, q)| encoding.map(|e| (e, q)))
        .filter(|(_, q)| q != &f32::NAN)
        .max_by(|(_, q1), (_, q2)| q1.partial_cmp(q2).unwrap_or(Ordering::Equal))
        .map(|(encoding, _)| encoding)
        .unwrap_or(Encoding::Identity)
}

async fn handle_http_request(
    env: Arc<Env>,
    req: Request<IncomingBody>,
) -> Result<Response<BoxBody>> {
    debug!("Received HTTP request: {req:?}");

    let http_response = route(env, req).await.or_else(|err: GenericError| {
        debug!("Unexpected error: {err}");

        http_response_builder(StatusCode::INTERNAL_SERVER_ERROR)
            .body(create_body("Internal Server Error", Encoding::Identity))
            .map_err(|err| err.into())
    });

    debug!("Sent HTTP response: {http_response:?}");

    http_response
}

fn http_response_builder<T>(status: T) -> response::Builder
where
    StatusCode: TryFrom<T>,
    <StatusCode as TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder()
        .status(status)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
}

async fn route(env: Arc<Env>, req: Request<IncomingBody>) -> Result<Response<BoxBody>> {
    let request_headers = req.headers().clone();

    let response_encoding = preferred_encoding(&request_headers);

    match (
        req.method(),
        req.uri().path(),
        req.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
    ) {
        (&Method::OPTIONS, _, _) => http_response_builder(StatusCode::OK)
            .header(header::ACCESS_CONTROL_ALLOW_HEADERS, "*")
            .header(header::ACCESS_CONTROL_ALLOW_METHODS, "POST, OPTIONS")
            .header(header::ACCESS_CONTROL_MAX_AGE, 86400)
            .body(create_body("", Encoding::Identity)),
        (&Method::POST, path, Some("application/json"))
            if path == format!("/{}/", env.config.rpc_token) =>
        {
            let request_body = req.collect().await?.aggregate();

            let rpc_response = match serde_json::from_reader::<_, RpcRequest>(request_body.reader())
            {
                Ok(rpc_request) => {
                    debug!("JSON-RPC Request: {rpc_request:?}");

                    handle_jsonrpc(env, &request_headers, &rpc_request)
                        .await
                        .unwrap_or_else(|_| {
                            fn error(id: Option<Id>) -> RpcResult {
                                RpcResult::error(id, -32603, "Internal error")
                            }

                            match rpc_request {
                                RpcRequest::Single(call) => RpcResponse::Single(error(call.id)),
                                RpcRequest::Batch(calls) => RpcResponse::Batch(
                                    calls.into_iter().map(|call| error(call.id)).collect(),
                                ),
                            }
                        })
                }
                Err(err) => {
                    debug!("JSON-RPC Invalid request: {err:?}");

                    RpcResponse::Single(RpcResult::error(None, -32600, "Invalid Request"))
                }
            };

            let response_body = serde_json::to_string(&rpc_response)?;

            debug!("JSON-RPC Response: {response_body}");

            http_response_builder(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::CONTENT_ENCODING,
                    response_encoding.to_header_value(),
                )
                .body(create_body(response_body, response_encoding))
        }
        (&Method::POST, _, Some("application/json")) => {
            http_response_builder(StatusCode::FORBIDDEN)
                .body(create_body("Access Denied", Encoding::Identity))
        }

        (&Method::POST, _, _) => http_response_builder(StatusCode::BAD_REQUEST)
            .body(create_body("Bad Request", Encoding::Identity)),

        (method, _, _) => http_response_builder(StatusCode::METHOD_NOT_ALLOWED).body(create_body(
            format!("Method {method} not allowed."),
            Encoding::Identity,
        )),
    }
    .map_err(|err| err.into())
}

#[derive(sqlx::FromRow)]
struct CachedRpcResult {
    key: String,
    response: String,
}

async fn get_from_cache(env: Arc<Env>, calls: &[&RpcCall]) -> HashMap<CacheKey, RpcResult> {
    let call_ids: HashMap<CacheKey, Option<Id>> = calls
        .iter()
        .filter_map(|call| get_cache_key(call).map(|cache_key| (cache_key, call.id.clone())))
        .collect();

    let cached_results =
        sqlx::query_as::<_, CachedRpcResult>("SELECT * FROM cached_response WHERE key = ANY($1)")
            .bind(
                call_ids
                    .keys()
                    .map(|CacheKey(string_value)| string_value.to_owned())
                    .collect::<Vec<String>>(),
            )
            .fetch_all(&env.db_pool)
            .await
            .unwrap_or_else(|err| {
                error!("Can't fetch from the db: {err}");
                vec![]
            });

    cached_results
        .into_iter()
        .filter_map(|cached_result| {
            let result = serde_json::from_str::<RpcResult>(&cached_result.response)
                .ok()
                .and_then(|rpc_result| rpc_result.result)?;

            let cache_key = CacheKey(cached_result.key);

            Some((
                cache_key.clone(),
                RpcResult {
                    error: None,
                    id: call_ids.get(&cache_key).cloned().flatten(),
                    jsonrpc: RPC_VERSION.to_string(),
                    result: Some(result),
                },
            ))
        })
        .collect()
}

async fn add_to_cache(
    env: Arc<Env>,
    calls: &[&RpcCall],
    results: &HashMap<Option<Id>, RpcResult>,
) -> Result<PgQueryResult> {
    let cacheable_results: Vec<(CacheKey, Value)> = calls
        .iter()
        .filter_map(|call| {
            let cache_key = get_cache_key(call)?;
            let result = results.get(&call.id)?;

            if result.result.is_some() {
                let json_value = serde_json::to_value(result).ok()?;

                Some((cache_key, json_value))
            } else {
                None
            }
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
    headers: &HeaderMap,
) -> Result<Value> {
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

    debug!("HTTP request to {url}: {request_builder:?}");

    let response = request_builder.send().await;

    debug!("HTTP response from {url}: {response:?}");

    let text = response?.text().await?;

    debug!("HTTP response body: {text}");

    serde_json::from_str(&text).map_err(|e| e.into())
}

async fn get_from_upstream(
    env: Arc<Env>,
    headers: &HeaderMap,
    calls: &[&RpcCall],
) -> Result<HashMap<Option<Id>, RpcResult>> {
    if let Some(rpc_request) = RpcRequest::new(calls.to_vec()) {
        let payload: Value = serde_json::to_value(rpc_request)?;

        debug!("HTTP request body: {payload}");

        let task: BoxFuture<Result<Value>> = env
            .config
            .solana_rpc_endpoints
            .iter()
            .map(|endpoint| http_post_json(Arc::clone(&env), endpoint.as_str(), &payload, headers))
            .fold(Box::pin(future::err("".into())), |acc, http_post| {
                Box::pin(acc.or_else(|_| http_post))
            });

        let json = task.await?;

        let rpc_response: RpcResponse = serde_json::from_value(json)?;

        Ok(rpc_response
            .results()
            .into_iter()
            .cloned()
            .map(|rpc_result| (rpc_result.id.clone(), rpc_result))
            .collect())
    } else {
        Ok(HashMap::new())
    }
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
    headers: &HeaderMap,
    rpc_request: &RpcRequest,
) -> Result<RpcResponse> {
    let (cacheable_calls, non_cacheable_calls): (Vec<&RpcCall>, Vec<&RpcCall>) = rpc_request
        .calls()
        .into_iter()
        .partition(|call| is_cacheable_call(call));

    let cached_results: HashMap<CacheKey, RpcResult> =
        get_from_cache(Arc::clone(&env), &cacheable_calls).await;

    let outstanding_calls: Vec<&RpcCall> = cacheable_calls
        .into_iter()
        .filter(|call| {
            get_cache_key(call)
                .map(|cache_key| !cached_results.contains_key(&cache_key))
                .unwrap_or(true)
        })
        .chain(non_cacheable_calls.into_iter())
        .collect();

    let upstream_results: HashMap<Option<Id>, RpcResult> =
        get_from_upstream(Arc::clone(&env), headers, &outstanding_calls).await?;

    add_to_cache(Arc::clone(&env), &outstanding_calls, &upstream_results).await?;

    let results: Vec<RpcResult> = rpc_request
        .calls()
        .into_iter()
        .map(|call| {
            get_cache_key(call)
                .and_then(|cache_key| cached_results.get(&cache_key))
                .or(upstream_results.get(&call.id))
                .cloned()
                .unwrap_or(RpcResult::error(call.id.clone(), -32603, "Internal error"))
        })
        .collect();

    info!(
        "JSON-RPC Response batch size: {} (from_cache={}, from_upstream={})",
        results.len(),
        cached_results.len(),
        upstream_results.len()
    );

    Ok(match (rpc_request, results.as_slice()) {
        (RpcRequest::Single(_), [result]) => RpcResponse::Single(result.clone()),
        (RpcRequest::Batch(calls), results) if calls.len() == results.len() => {
            RpcResponse::Batch(results.to_vec())
        }
        _ => RpcResponse::Single(RpcResult::error(None, -32603, "Internal error")),
    })
}
