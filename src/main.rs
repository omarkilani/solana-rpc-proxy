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
use serde::Deserialize;
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

            let request_json: Value = serde_json::from_reader(request_body.reader())?;

            debug!("JSON-RPC Request: {}", request_json);

            let response_json: Value = handle_jsonrpc(env, request_headers, request_json).await?;

            let response_body = serde_json::to_string(&response_json)?;

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
struct CachedResponse {
    key: String,
    response: String,
}

async fn get_from_cache(env: Arc<Env>, requests: Vec<&Value>) -> HashMap<String, Value> {
    let keys: Vec<String> = requests.into_iter().filter_map(get_cache_key).collect();

    sqlx::query_as::<_, CachedResponse>("SELECT * FROM cached_response WHERE key = ANY($1)")
        .bind(keys)
        .fetch_all(&env.db_pool)
        .await
        .map_or(HashMap::new(), |cached_responses| {
            cached_responses
                .into_iter()
                .filter_map(|CachedResponse { key, response }| {
                    serde_json::from_str(&response)
                        .ok()
                        .map(|value| (key, value))
                })
                .collect()
        })
}

async fn add_to_cache(
    env: Arc<Env>,
    requests: &Vec<(usize, &Value)>,
    responses: &HashMap<usize, Value>,
) -> Result<PgQueryResult> {
    let responses_by_key: Vec<(String, &Value)> = requests
        .into_iter()
        .filter_map(|(i, request)| get_cache_key(request).map(|key| (i, key)))
        .filter_map(|(i, key)| responses.get(i).map(|response| (key, response)))
        .collect();

    if responses_by_key.is_empty() {
        Ok(PgQueryResult::default())
    } else {
        QueryBuilder::new("INSERT INTO cached_response(key, response) ")
            .push_values(responses_by_key, |mut builder, (key, response)| {
                builder.push_bind(key).push_bind(response);
            })
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
    requests: &Vec<(usize, &Value)>,
) -> Result<HashMap<usize, Value>> {
    let is_batch = requests.len() > 1;

    let payload: Value = serde_json::to_value::<Vec<&Value>>(
        requests.into_iter().map(|(_, request)| *request).collect(),
    )?;

    let json: Value = http_post_json(
        Arc::clone(&env),
        env.config.solana_rpc_endpoint.as_str(),
        if is_batch { &payload } else { &payload[0] },
        headers,
    )
    .await?;

    let responses: Vec<Value> = match json {
        Value::Array(arr) if is_batch => Ok(arr),
        value if !is_batch => Ok(vec![value]),
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
                .as_i64()
                .unwrap_or(-1);

            Some(format!("{method}::{txn_id}::{max_supported_txn_version}"))
        }
        _ => None,
    })
}

async fn handle_jsonrpc(env: Arc<Env>, headers: HeaderMap, json: Value) -> Result<Value> {
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
        Arc::clone(&env),
        cacheable_requests
            .iter()
            .map(|(_, request)| *request)
            .collect(),
    )
    .await;

    let outstanding_requests: Vec<(usize, &Value)> = cacheable_requests
        .into_iter()
        .filter(|(_, request)| {
            !cached_responses.contains_key(&get_cache_key(request).unwrap_or(String::new()))
        })
        .chain(non_cacheable_requests.into_iter())
        .collect();

    let upstream_responses: HashMap<usize, Value> = if outstanding_requests.is_empty() {
        HashMap::new()
    } else {
        get_from_upstream(Arc::clone(&env), headers, &outstanding_requests).await?
    };

    if !upstream_responses.is_empty() {
        add_to_cache(Arc::clone(&env), &outstanding_requests, &upstream_responses).await?;
    }

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

    info!(
        "JSON-RPC Response batch size: {} (from_cache={}, from_upstream={})",
        all_responses.len(),
        cached_responses.len(),
        upstream_responses.len()
    );

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
