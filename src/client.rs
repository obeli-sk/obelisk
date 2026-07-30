use grpc::{grpc_gen, injector::TracingInjector};
use secrecy::{ExposeSecret as _, SecretString};
use tonic::{codec::CompressionEncoding, transport::Channel};

/// Resolved client-side startup state, threaded through client subcommands instead of
/// a process global. Holds the token this CLI invocation presents to the server,
/// resolved once from `--api-token` > `OBELISK_API_TOKEN` > `OBELISK__API__TOKEN`, and
/// builds gRPC and web-API clients that inject it.
#[derive(Clone, Default)]
pub(crate) struct ClientStartup {
    api_token: Option<SecretString>,
}

impl ClientStartup {
    pub(crate) fn new(flag: Option<SecretString>) -> Self {
        Self {
            api_token: resolve_api_token(flag),
        }
    }

    fn interceptor(&self) -> Result<ClientInterceptor, anyhow::Error> {
        ClientInterceptor::new(self.api_token.as_ref())
    }

    /// HTTP client for web-API calls, presenting the API token as a default header.
    pub(crate) fn web_api_client(&self) -> Result<reqwest::Client, anyhow::Error> {
        Ok(self.web_api_client_builder()?.build()?)
    }

    pub(crate) fn web_api_client_builder(&self) -> Result<reqwest::ClientBuilder, anyhow::Error> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &self.api_token {
            let mut value = reqwest::header::HeaderValue::from_str(&format!(
                "Bearer {}",
                token.expose_secret()
            ))
            .map_err(|_| anyhow::anyhow!("API token contains invalid header characters"))?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        Ok(reqwest::Client::builder().default_headers(headers))
    }

    pub(crate) fn execution_repository_client(
        &self,
        channel: Channel,
    ) -> Result<ExecutionRepositoryClient, anyhow::Error> {
        Ok(
            grpc_gen::execution_repository_client::ExecutionRepositoryClient::with_interceptor(
                channel,
                self.interceptor()?,
            )
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip),
        )
    }

    pub(crate) fn deployment_repository_client(
        &self,
        channel: Channel,
    ) -> Result<DeploymentRepositoryClient, anyhow::Error> {
        Ok(
            grpc_gen::deployment_repository_client::DeploymentRepositoryClient::with_interceptor(
                channel,
                self.interceptor()?,
            )
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_encoding_message_size(crate::api::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(crate::api::MAX_GRPC_MESSAGE_SIZE),
        )
    }

    pub(crate) fn fn_repository_client(
        &self,
        channel: Channel,
    ) -> Result<FunctionRepositoryClient, anyhow::Error> {
        Ok(
            grpc_gen::function_repository_client::FunctionRepositoryClient::with_interceptor(
                channel,
                self.interceptor()?,
            )
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip),
        )
    }
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

/// Client interceptor for all gRPC calls: injects tracing metadata and, if a
/// token is configured, the `authorization` header.
#[derive(Clone)]
pub(crate) struct ClientInterceptor {
    authorization: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl ClientInterceptor {
    fn new(token: Option<&SecretString>) -> Result<Self, anyhow::Error> {
        let authorization = if let Some(token) = token {
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

pub(crate) type ExecutionRepositoryClient =
    grpc_gen::execution_repository_client::ExecutionRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;

type DeploymentRepositoryClient =
    grpc_gen::deployment_repository_client::DeploymentRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;

pub(crate) type FunctionRepositoryClient =
    grpc_gen::function_repository_client::FunctionRepositoryClient<
        tonic::service::interceptor::InterceptedService<Channel, ClientInterceptor>,
    >;
