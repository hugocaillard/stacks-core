use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use stacks::prometheus::{gather, Encoder, TextEncoder};

use super::MonitoringError;

pub fn start_serving_prometheus_metrics(bind_address: String) -> Result<(), MonitoringError> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let addr: SocketAddr = bind_address.parse().map_err(|_| {
            warn!("Prometheus monitoring: unable to parse bind address, will not spawn prometheus endpoint service.");
            MonitoringError::AlreadyBound
        })?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|_| {
                warn!("Prometheus monitoring: unable to bind address, will not spawn prometheus endpoint service.");
                MonitoringError::AlreadyBound
            })?;

        let local_addr = listener
            .local_addr()
            .map_err(|_| {
                warn!("Prometheus monitoring: unable to get local bind address, will not spawn prometheus endpoint service.");
                MonitoringError::UnableToGetAddress
            })?;
        info!("Prometheus monitoring: server listening on http://{local_addr}");

        loop {
            let (stream, _) = listener.accept().await.map_err(|err| {
                error!("Prometheus monitoring: unable to accept connection - {err:?}");
                MonitoringError::AlreadyBound
            })?;

            let io = TokioIo::new(stream);
            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service_fn(handle_request))
                    .await
                {
                    error!("Error serving prometheus metrics: {err:?}");
                }
            });
        }
    })
}

async fn handle_request(_req: Request<hyper::body::Incoming>) -> Result<Response<String>, Infallible> {
    debug!("Handle Prometheus polling");

    let encoder = TextEncoder::new();
    let metric_families = gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", encoder.format_type())
        .body(String::from_utf8(buffer).unwrap())
        .unwrap();

    Ok(response)
}
