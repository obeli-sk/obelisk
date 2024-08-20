use anyhow::Context;
use tonic::transport::{Channel, ClientTlsConfig};

pub(crate) mod grpc_mapping;

pub(crate) type TonicResult<T> = Result<T, tonic::Status>;

pub(crate) type TonicRespResult<T> = TonicResult<tonic::Response<T>>;

pub(crate) async fn to_channel(url: String) -> Result<Channel, anyhow::Error> {
    let tls = ClientTlsConfig::new().with_native_roots();
    let url = url.parse().context("cannot parse uri")?;
    Channel::builder(url)
        .tls_config(tls)?
        .connect()
        .await
        .context("connect error")
}
