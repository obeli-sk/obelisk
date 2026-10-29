//! Outbound HTTP client of activities and webhooks, optionally reusing idle HTTP/1 connections.

use http_body_util::BodyExt;
use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::http::uri::Scheme;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use wasmtime_wasi_http::p2::body::{HyperIncomingBody, HyperOutgoingBody};
use wasmtime_wasi_http::{Error, RequestOptions, default_send_request};

/// Matches the timeouts of [`default_send_request`] when the guest sets none.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);
/// Expire ahead of the server's `Keep-Alive: timeout` so a request does not race its close.
const KEEP_ALIVE_HINT_MARGIN: Duration = Duration::from_secs(1);
const KEEP_ALIVE_HINT_MAX: Duration = Duration::from_secs(600);

pub(crate) type IoFuture = Box<dyn Future<Output = Result<(), Error>> + Send>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpPoolConfig {
    /// Idle connections kept per scheme and authority; `0` opens a new connection per request.
    pub max_idle_per_host: usize,
    /// Used when the server sends no `Keep-Alive: timeout` hint.
    pub idle_timeout: Duration,
}

impl Default for HttpPoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 16,
            idle_timeout: Duration::from_secs(4),
        }
    }
}

/// Cheaply cloneable handle; clones share the same pool.
#[derive(Clone)]
pub struct OutboundHttpClient(Option<Arc<Pool>>);

impl Default for OutboundHttpClient {
    fn default() -> Self {
        Self::new(HttpPoolConfig::default())
    }
}

impl std::fmt::Debug for OutboundHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OutboundHttpClient")
            .field(&self.0.as_ref().map(|pool| pool.config))
            .finish()
    }
}

impl OutboundHttpClient {
    #[must_use]
    /// Connections outlive individual isolates and run on the runtime that creates the pool.
    pub fn new(config: HttpPoolConfig) -> Self {
        Self((config.max_idle_per_host > 0).then(|| {
            Arc::new(Pool {
                config,
                idle: Mutex::default(),
                tls: OnceLock::new(),
                runtime: wasmtime_wasi::runtime::with_ambient_tokio_runtime(
                    tokio::runtime::Handle::current,
                ),
            })
        }))
    }

    pub(crate) async fn send(
        &self,
        request: hyper::Request<HyperOutgoingBody>,
        options: Option<RequestOptions>,
    ) -> Result<(hyper::Response<HyperIncomingBody>, IoFuture), Error> {
        if let Some(pool) = &self.0 {
            let response = pool.send(request, options).await?;
            Ok((response, Box::new(async { Ok(()) })))
        } else {
            let (response, io) = default_send_request(request, options).await?;
            Ok((response.map(BodyExt::boxed_unsync), Box::new(io)))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PoolKey {
    use_tls: bool,
    /// Always `host:port`.
    authority: String,
}

impl PoolKey {
    fn new(uri: &hyper::Uri) -> Result<Self, Error> {
        let authority = uri.authority().ok_or(Error::HttpRequestUriInvalid)?;
        let use_tls = uri.scheme() == Some(&Scheme::HTTPS);
        let authority = if authority.port().is_some() {
            authority.to_string()
        } else {
            format!("{authority}:{}", if use_tls { 443 } else { 80 })
        };
        Ok(Self { use_tls, authority })
    }
}

struct IdleConnection {
    sender: SendRequest<HyperOutgoingBody>,
    expires_at: Instant,
}

struct Pool {
    config: HttpPoolConfig,
    idle: Mutex<HashMap<PoolKey, Vec<IdleConnection>>>,
    tls: OnceLock<tokio_rustls::TlsConnector>,
    runtime: tokio::runtime::Handle,
}

struct Timeouts {
    connect: Duration,
    first_byte: Duration,
    between_bytes: Duration,
}

impl From<Option<RequestOptions>> for Timeouts {
    fn from(options: Option<RequestOptions>) -> Self {
        let options = options.unwrap_or_default();
        Self {
            connect: options.connect_timeout.unwrap_or(DEFAULT_TIMEOUT),
            first_byte: options.first_byte_timeout.unwrap_or(DEFAULT_TIMEOUT),
            // Tokio panics on a zero `interval`.
            between_bytes: options
                .between_bytes_timeout
                .unwrap_or(DEFAULT_TIMEOUT)
                .max(Duration::from_nanos(1)),
        }
    }
}

impl Pool {
    async fn send(
        self: &Arc<Self>,
        mut request: hyper::Request<HyperOutgoingBody>,
        options: Option<RequestOptions>,
    ) -> Result<hyper::Response<HyperIncomingBody>, Error> {
        let timeouts = Timeouts::from(options);
        let key = PoolKey::new(request.uri())?;
        // The request line carries only the path unless addressing a proxy.
        *request.uri_mut() = hyper::Uri::builder()
            .path_and_query(
                request
                    .uri()
                    .path_and_query()
                    .map_or("/", |path_and_query| path_and_query.as_str()),
            )
            .build()
            .expect("comes from a valid request");
        loop {
            let (mut sender, reused) = match self.checkout(&key).await {
                Some(sender) => (sender, true),
                None => (self.connect(&key, timeouts.connect).await?, false),
            };
            let response =
                tokio::time::timeout(timeouts.first_byte, sender.try_send_request(request))
                    .await
                    .map_err(|_| Error::ConnectionReadTimeout)?;
            match response {
                Ok(response) => {
                    let idle_timeout = self.idle_timeout(response.headers());
                    let mut between_bytes = tokio::time::interval(timeouts.between_bytes);
                    between_bytes.reset();
                    let (parts, incoming) = response.into_parts();
                    let mut body = PooledBody {
                        incoming,
                        between_bytes,
                        release: Some((self.clone(), key, sender, idle_timeout)),
                    };
                    // A body that is never polled (e.g. 204 or HEAD) must still release.
                    if body.incoming.is_end_stream() {
                        body.release();
                    }
                    return Ok(hyper::Response::from_parts(parts, body.boxed_unsync()));
                }
                Err(mut err) => match err.take_message() {
                    // The peer may have closed an idle connection before the request was written.
                    Some(unsent) if reused => request = unsent,
                    _ => return Err(Error::from(err.into_error())),
                },
            }
        }
    }

    async fn checkout(&self, key: &PoolKey) -> Option<SendRequest<HyperOutgoingBody>> {
        loop {
            let mut sender = self.pop_idle(key)?;
            // A just-released connection may still be finishing its previous exchange.
            if sender.ready().await.is_ok() {
                return Some(sender);
            }
        }
    }

    fn pop_idle(&self, key: &PoolKey) -> Option<SendRequest<HyperOutgoingBody>> {
        let mut idle = self.idle.lock().expect("pool mutex not poisoned");
        let connections = idle.get_mut(key)?;
        let now = Instant::now();
        connections
            .retain(|connection| !connection.sender.is_closed() && now < connection.expires_at);
        let sender = connections.pop().map(|connection| connection.sender);
        if connections.is_empty() {
            idle.remove(key);
        }
        sender
    }

    fn idle_timeout(&self, headers: &hyper::HeaderMap) -> Duration {
        keep_alive_timeout(headers).map_or(self.config.idle_timeout, |hint| {
            hint.saturating_sub(KEEP_ALIVE_HINT_MARGIN)
                .min(KEEP_ALIVE_HINT_MAX)
        })
    }

    fn checkin(
        self: Arc<Self>,
        key: PoolKey,
        sender: SendRequest<HyperOutgoingBody>,
        idle_timeout: Duration,
    ) {
        if idle_timeout.is_zero() {
            return;
        }
        let expires_at = Instant::now() + idle_timeout;
        {
            let mut idle = self.idle.lock().expect("pool mutex not poisoned");
            let connections = idle.entry(key.clone()).or_default();
            if connections.len() >= self.config.max_idle_per_host {
                connections.remove(0);
            }
            connections.push(IdleConnection { sender, expires_at });
        }
        self.runtime.clone().spawn(async move {
            tokio::time::sleep_until(expires_at.into()).await;
            self.evict_expired(&key);
        });
    }

    fn evict_expired(&self, key: &PoolKey) {
        let mut idle = self.idle.lock().expect("pool mutex not poisoned");
        if let Some(connections) = idle.get_mut(key) {
            let now = Instant::now();
            connections
                .retain(|connection| !connection.sender.is_closed() && now < connection.expires_at);
            if connections.is_empty() {
                idle.remove(key);
            }
        }
    }

    async fn connect(
        self: &Arc<Self>,
        key: &PoolKey,
        connect_timeout: Duration,
    ) -> Result<SendRequest<HyperOutgoingBody>, Error> {
        let pool = self.clone();
        let key = key.clone();
        let mut task: wasmtime_wasi::runtime::AbortOnDropJoinHandle<_> = self
            .runtime
            .spawn(async move {
                tokio::time::timeout(connect_timeout, async {
                    let stream = TcpStream::connect(&key.authority)
                        .await
                        .map_err(Error::Connect)?;
                    if key.use_tls {
                        let domain =
                            tls_server_name(&key.authority).map_err(Error::InvalidDnsNameError)?;
                        let stream = pool
                            .tls_connector()
                            .connect(domain, stream)
                            .await
                            .map_err(Error::Tls)?;
                        handshake(stream).await
                    } else {
                        handshake(stream).await
                    }
                })
                .await
                .map_err(|_| Error::ConnectionTimeout)?
            })
            .into();
        (&mut *task)
            .await
            .map_err(|error| Error::InternalError(Some(error.to_string())))?
    }

    fn tls_connector(&self) -> &tokio_rustls::TlsConnector {
        self.tls.get_or_init(|| {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            };
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            tokio_rustls::TlsConnector::from(Arc::new(config))
        })
    }
}

async fn handshake(
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
) -> Result<SendRequest<HyperOutgoingBody>, Error> {
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!("Outbound HTTP connection failed: {err}");
        }
    });
    Ok(sender)
}

/// Seconds from the `timeout` parameter of a `Keep-Alive` response header, e.g. `timeout=5, max=100`.
fn keep_alive_timeout(headers: &hyper::HeaderMap) -> Option<Duration> {
    headers
        .get_all("keep-alive")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .find_map(|param| {
            let (name, value) = param.split_once('=')?;
            if name.trim().eq_ignore_ascii_case("timeout") {
                value.trim().trim_matches('"').parse().ok()
            } else {
                None
            }
        })
        .map(Duration::from_secs)
}

/// Same as wasmtime's: IP literals (including bracketed IPv6) or a host name without the port.
fn tls_server_name(
    authority: &str,
) -> Result<rustls::pki_types::ServerName<'static>, rustls::pki_types::InvalidDnsNameError> {
    use rustls::pki_types::ServerName;
    if let Ok(addr) = authority.parse::<std::net::SocketAddr>() {
        return Ok(ServerName::from(addr.ip()));
    }
    let host = authority
        .split_once(':')
        .map_or(authority, |(host, _)| host);
    Ok(ServerName::try_from(host)?.to_owned())
}

/// Response body returning its connection to the pool once fully read.
struct PooledBody {
    incoming: Incoming,
    between_bytes: tokio::time::Interval,
    release: Option<(Arc<Pool>, PoolKey, SendRequest<HyperOutgoingBody>, Duration)>,
}

impl PooledBody {
    fn release(&mut self) {
        if let Some((pool, key, sender, idle_timeout)) = self.release.take() {
            pool.checkin(key, sender, idle_timeout);
        }
    }
}

impl Body for PooledBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.incoming).poll_frame(cx) {
            Poll::Ready(None) => {
                this.release();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(frame))) => {
                this.between_bytes.reset();
                if this.incoming.is_end_stream() {
                    this.release();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.release = None;
                Poll::Ready(Some(Err(if err.is_timeout() {
                    Error::HttpResponseTimeout
                } else {
                    Error::from(err)
                })))
            }
            Poll::Pending => {
                ready!(this.between_bytes.poll_tick(cx));
                this.release = None;
                Poll::Ready(Some(Err(Error::ConnectionReadTimeout)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.incoming.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.incoming.size_hint()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Serves `200 ok` over plain HTTP/1, counting accepted connections.
    pub(crate) async fn server() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
        server_with_keep_alive(None).await
    }

    async fn server_with_keep_alive(
        keep_alive: Option<&'static str>,
    ) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(
                    TokioIo::new(stream),
                    hyper::service::service_fn(move |_| async move {
                        let mut response = hyper::Response::new(http_body_util::Full::new(
                            Bytes::from_static(b"ok"),
                        ));
                        if let Some(keep_alive) = keep_alive {
                            response.headers_mut().insert(
                                "keep-alive",
                                hyper::header::HeaderValue::from_static(keep_alive),
                            );
                        }
                        Ok::<_, std::convert::Infallible>(response)
                    }),
                ));
            }
        });
        (addr, accepted)
    }

    async fn get_sequentially(client: &OutboundHttpClient, addr: std::net::SocketAddr) {
        for _ in 0..3 {
            let request = hyper::Request::builder()
                .uri(format!("http://{addr}/"))
                .header(hyper::header::HOST, addr.to_string())
                .body(
                    http_body_util::Empty::new()
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .unwrap();
            let (response, _io) = client.send(request, None).await.unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(b"ok", body.as_ref());
        }
    }

    #[tokio::test]
    async fn pooled_client_reuses_connection() {
        let (addr, accepted) = server().await;
        get_sequentially(&OutboundHttpClient::default(), addr).await;
        assert_eq!(1, accepted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn zero_max_idle_opens_connection_per_request() {
        let (addr, accepted) = server().await;
        let client = OutboundHttpClient::new(HttpPoolConfig {
            max_idle_per_host: 0,
            ..HttpPoolConfig::default()
        });
        get_sequentially(&client, addr).await;
        assert_eq!(3, accepted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn pooled_connection_survives_blocked_origin_runtime() {
        let (addr, accepted) = server().await;
        let client = OutboundHttpClient::default();
        let origin_client = client.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let origin = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                get_sequentially(&origin_client, addr).await;
                ready_tx.send(()).unwrap();
                // Native V8 blocks its thread while a host call runs on the server runtime.
                release_rx.recv().unwrap();
            });
        });
        ready_rx.await.unwrap();
        let result =
            tokio::time::timeout(Duration::from_secs(2), get_sequentially(&client, addr)).await;
        release_tx.send(()).unwrap();
        tokio::task::spawn_blocking(move || origin.join().unwrap())
            .await
            .unwrap();
        assert!(
            result.is_ok(),
            "pooled connection depends on the blocked runtime"
        );
        assert_eq!(1, accepted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn keep_alive_hint_shortens_idle_timeout() {
        let (addr, accepted) = server_with_keep_alive(Some("max=100, timeout=1")).await;
        get_sequentially(&OutboundHttpClient::default(), addr).await;
        assert_eq!(3, accepted.load(Ordering::SeqCst));
    }

    #[test]
    fn keep_alive_timeout_parses_timeout_parameter() {
        let mut headers = hyper::HeaderMap::new();
        assert_eq!(None, keep_alive_timeout(&headers));
        headers.insert("keep-alive", "max=100, Timeout=5".parse().unwrap());
        assert_eq!(Some(Duration::from_secs(5)), keep_alive_timeout(&headers));
    }
}
