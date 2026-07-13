use grpc::{grpc_gen, injector::TracingInjector};
use secrecy::{ExposeSecret as _, SecretString};
use tonic::{codec::CompressionEncoding, transport::Channel};

/// Token this CLI invocation presents to the server, resolved once from
/// `--api-token` > `OBELISK_API_TOKEN` > `OBELISK__API__TOKEN`.
static API_TOKEN: std::sync::OnceLock<Option<SecretString>> = std::sync::OnceLock::new();

pub(crate) fn init_api_token(flag: Option<SecretString>) {
    API_TOKEN
        .set(resolve_api_token(flag))
        .expect("API_TOKEN is only set here");
}

fn resolve_api_token(flag: Option<SecretString>) -> Option<SecretString> {
    flag.or_else(|| {
        std::env::var("OBELISK_API_TOKEN")
            .ok()
            .map(SecretString::from)
    })
    .or_else(|| {
        std::env::var("OBELISK__API__TOKEN")
            .ok()
            .map(SecretString::from)
    })
    .filter(|token| !token.expose_secret().is_empty())
}

fn api_token() -> Option<&'static SecretString> {
    API_TOKEN.get_or_init(|| None).as_ref()
}

/// Client interceptor for all gRPC calls: injects tracing metadata and, if a
/// token is configured, the `authorization` header.
#[derive(Clone)]
pub(crate) struct ClientInterceptor {
    authorization: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl ClientInterceptor {
    fn new() -> Result<Self, anyhow::Error> {
        let authorization = if let Some(token) = api_token() {
            let mut authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {}", token.expose_secret())
                    .parse()
                    .map_err(|_| anyhow::anyhow!("API token contains invalid header characters"))?;
            authorization.set_sensitive(true);
            Some(authorization)
        } else {
            None
        };
        Ok(Self { authorization })
    }
}

impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = tonic::service::Interceptor::call(&mut TracingInjector, request)?;
        if let Some(authorization) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }
        Ok(request)
    }
}

/// HTTP client for web-API calls, presenting the API token as a default header.
pub(crate) fn web_api_client() -> Result<reqwest::Client, anyhow::Error> {
    Ok(web_api_client_builder()?.build()?)
}

pub(crate) fn web_api_client_builder() -> Result<reqwest::ClientBuilder, anyhow::Error> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = api_token() {
        let mut value =
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                .map_err(|_| anyhow::anyhow!("API token contains invalid header characters"))?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    Ok(reqwest::Client::builder().default_headers(headers))
}

pub(crate) type ExecutionRepositoryClient =
    grpc_gen::execution_repository_client::ExecutionRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;

pub(crate) async fn get_execution_repository_client(
    channel: Channel,
) -> Result<ExecutionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::execution_repository_client::ExecutionRepositoryClient::with_interceptor(
            channel,
            ClientInterceptor::new()?,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip),
    )
}

type DeploymentRepositoryClient =
    grpc_gen::deployment_repository_client::DeploymentRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;

pub(crate) async fn get_deployment_repository_client(
    channel: Channel,
) -> Result<DeploymentRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::deployment_repository_client::DeploymentRepositoryClient::with_interceptor(
            channel,
            ClientInterceptor::new()?,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_encoding_message_size(crate::api::MAX_GRPC_MESSAGE_SIZE)
        .max_decoding_message_size(crate::api::MAX_GRPC_MESSAGE_SIZE),
    )
}

pub(crate) type FunctionRepositoryClient =
    grpc_gen::function_repository_client::FunctionRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;

pub(crate) async fn get_fn_repository_client(
    channel: Channel,
) -> Result<FunctionRepositoryClient, anyhow::Error> {
    Ok(
        grpc_gen::function_repository_client::FunctionRepositoryClient::with_interceptor(
            channel,
            ClientInterceptor::new()?,
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip),
    )
}
