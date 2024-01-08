use crate::configs::DQConfig;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::FlightSqlService;
use tonic::transport::Server;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

pub async fn serve(
    config: DQConfig,
    service: impl FlightSqlService,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = if let Some(tls_config) = config.tls.as_ref() {
        let server_cert = std::fs::read_to_string(tls_config.server_cert.clone())?;
        let server_key = std::fs::read_to_string(tls_config.server_key.clone())?;
        let client_cert = std::fs::read_to_string(tls_config.client_cert.clone())?;

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&server_cert, &server_key))
            .client_ca_root(Certificate::from_pem(&client_cert));

        Server::builder().tls_config(tls_config)?
    } else {
        Server::builder()
    };

    let router = server.add_service(FlightServiceServer::new(service));

    let listen = config.listen.parse()?;

    log::info!("listening on {:?}", listen);

    router.serve(listen).await?;

    Ok(())
}
