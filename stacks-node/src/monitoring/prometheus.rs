use std::thread;

use stacks::prometheus::{gather, Encoder, TextEncoder};
use tiny_http::{Header, Response, Server};

use super::MonitoringError;

pub fn start_serving_prometheus_metrics(bind_address: String) -> Result<(), MonitoringError> {
    let server = Server::http(&bind_address).map_err(|e| {
        warn!("Prometheus monitoring: unable to bind to address {bind_address}: {e}");
        MonitoringError::AlreadyBound
    })?;

    let local_addr = server.server_addr().to_string();
    info!("Prometheus monitoring: server listening on http://{local_addr}");

    loop {
        let request = match server.recv() {
            Ok(request) => request,
            Err(err) => {
                error!("Prometheus monitoring: unable to receive request - {err:?}",);
                continue;
            }
        };

        if let Err(e) = thread::Builder::new()
            .name("prometheus-handler".to_string())
            .spawn(move || {
                if let Err(err) = handle_request(request) {
                    error!("Prometheus monitoring: error handling request - {err:?}");
                }
            })
        {
            error!("Prometheus monitoring: failed to spawn handler thread - {e}");
        }
    }
}

fn handle_request(request: tiny_http::Request) -> Result<(), MonitoringError> {
    debug!("Handle Prometheus polling ({:?})", request.remote_addr());

    let encoder = TextEncoder::new();
    let metric_families = gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer)
        .map_err(|e| MonitoringError::EncodingError(format!("Failed to encode metrics: {}", e)))?;

    let content_type_header = Header::from_bytes(&b"Content-Type"[..], encoder.format_type())
        .map_err(|e| MonitoringError::ResponseError(format!("Failed to create content-type header: {:?}", e)))?;

    let response = Response::from_data(buffer).with_header(content_type_header);

    request.respond(response)
        .map_err(|e| MonitoringError::ResponseError(format!("Failed to send response: {}", e)))?;
    Ok(())
}
