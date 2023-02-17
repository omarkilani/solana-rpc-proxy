use bytes::{Buf, Bytes};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Incoming as IncomingBody, header, Method, Request, Response, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

struct RequestMeta {
    request: Value,
    is_cacheable: bool,
}

struct IncomingRequest {
    was_single_rpc: bool,
    has_cacheable_methods: bool,
    requests: Vec<Value>,
    annotated_requests: Vec<RequestMeta>,
    responses: Vec<Value>,
}

impl From<Value> for IncomingRequest {
    fn from(value: Value) -> IncomingRequest {
        let (requests, was_single_rpc) = if value.is_array() {
            (value.as_array().expect("the impossible").to_vec(), false)
        } else {
            (vec![value], true)
        };

        let mut annotated_requests: Vec<RequestMeta> = vec![];

        let has_cacheable_methods = requests.iter().cloned().fold(false, |result, request| {
            let is_cacheable = request["method"] == "getTransaction";

            annotated_requests.push(RequestMeta {
                request: request,
                is_cacheable: is_cacheable,
            });

            result || is_cacheable
        });

        IncomingRequest {
            was_single_rpc,
            has_cacheable_methods,
            requests,
            annotated_requests,
            responses: vec![],
        }
    }
}

impl From<IncomingRequest> for Value {
    fn from(incoming_request: IncomingRequest) -> Value {
        if incoming_request.was_single_rpc {
            incoming_request.requests[0].clone()
        } else {
            Value::Array(incoming_request.requests)
        }
    }
}

async fn handle(config: Arc<Config>, req: Request<IncomingBody>) -> Result<Response<BoxBody>> {
    let headers = req.headers().clone();

    let body = req.collect().await?.aggregate();

    let data: Value = serde_json::from_reader(body.reader())?;

    let incoming_request = IncomingRequest::from(data);

    if !incoming_request.has_cacheable_methods {
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full("TODO"))?;

        Ok(response)
    } else {
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full("TODO"))?;

        Ok(response)
    }
}

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

        let config = config.clone();

        tokio::task::spawn(async move {
            let service = service_fn(move |req| route(config.clone(), req));

            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service)
                .await
            {
                println!("Failed to serve connection: {:?}", err);
            }
        });
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    rpc_token: String,
    solana_rpc_endpoint: String,
}
