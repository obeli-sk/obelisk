#[allow(dead_code)]
pub mod obelisk {
    #[allow(dead_code)]
    pub mod types {
        #[allow(dead_code, clippy::all)]
        pub mod time {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            /// A duration of time, in nanoseconds.
            /// Extracted from wasi:clocks@0.2.0 to avoid dependency on wasi:io
            pub type Duration = u64;
        }
        #[allow(dead_code, clippy::all)]
        pub mod execution {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type JoinSetId = _rt::String;
        }
    }
    #[allow(dead_code)]
    pub mod workflow {
        #[allow(dead_code, clippy::all)]
        pub mod host_activities {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type Duration = super::super::super::obelisk::types::time::Duration;
            pub type JoinSetId = super::super::super::obelisk::types::execution::JoinSetId;
            #[allow(unused_unsafe, clippy::all)]
            pub fn sleep(nanos: Duration) {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "sleep"]
                        fn wit_import(_: i64);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i64) {
                        unreachable!()
                    }
                    wit_import(_rt::as_i64(nanos));
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn new_join_set() -> JoinSetId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "new-join-set"]
                        fn wit_import(_: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0);
                    let l1 = *ptr0.add(0).cast::<*mut u8>();
                    let l2 = *ptr0.add(4).cast::<usize>();
                    let len3 = l2;
                    let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                    _rt::string_lift(bytes3)
                }
            }
        }
    }
}
#[allow(dead_code)]
pub mod testing {
    #[allow(dead_code)]
    pub mod fibo_workflow {
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibow(n: u8, iterations: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow")]
                    extern "C" {
                        #[link_name = "fibow"]
                        fn wit_import(_: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n), _rt::as_i32(&iterations));
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa(n: u8, iterations: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow")]
                    extern "C" {
                        #[link_name = "fiboa"]
                        fn wit_import(_: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n), _rt::as_i32(&iterations));
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_concurrent(n: u8, iterations: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-concurrent"]
                        fn wit_import(_: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n), _rt::as_i32(&iterations));
                    ret as u64
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod fibo_workflow_obelisk_ext {
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_submit(join_set_id: &str, n: u8, iterations: u32) -> _rt::String {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-submit"]
                        fn wit_import(_: *mut u8, _: usize, _: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        ptr0.cast_mut(),
                        len0,
                        _rt::as_i32(&n),
                        _rt::as_i32(&iterations),
                        ptr1,
                    );
                    let l2 = *ptr1.add(0).cast::<*mut u8>();
                    let l3 = *ptr1.add(4).cast::<usize>();
                    let len4 = l3;
                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                    _rt::string_lift(bytes4)
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_await_next(join_set_id: &str) -> u64 {
                unsafe {
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-await-next"]
                        fn wit_import(_: *mut u8, _: usize) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(ptr0.cast_mut(), len0);
                    ret as u64
                }
            }
        }
    }
}
#[allow(dead_code)]
pub mod wasi {
    #[allow(dead_code)]
    pub mod clocks {
        #[allow(dead_code, clippy::all)]
        pub mod monotonic_clock {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type Pollable = super::super::super::wasi::io::poll::Pollable;
            /// An instant in time, in nanoseconds. An instant is relative to an
            /// unspecified initial value, and can only be compared to instances from
            /// the same monotonic-clock.
            pub type Instant = u64;
            /// A duration of time, in nanoseconds.
            pub type Duration = u64;
            #[allow(unused_unsafe, clippy::all)]
            /// Read the current value of the clock.
            ///
            /// The clock is monotonic, therefore calling this function repeatedly will
            /// produce a sequence of non-decreasing values.
            pub fn now() -> Instant {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:clocks/monotonic-clock@0.2.0")]
                    extern "C" {
                        #[link_name = "now"]
                        fn wit_import() -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import() -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import();
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Query the resolution of the clock. Returns the duration of time
            /// corresponding to a clock tick.
            pub fn resolution() -> Duration {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:clocks/monotonic-clock@0.2.0")]
                    extern "C" {
                        #[link_name = "resolution"]
                        fn wit_import() -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import() -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import();
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Create a `pollable` which will resolve once the specified instant
            /// occured.
            pub fn subscribe_instant(when: Instant) -> Pollable {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:clocks/monotonic-clock@0.2.0")]
                    extern "C" {
                        #[link_name = "subscribe-instant"]
                        fn wit_import(_: i64) -> i32;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i64) -> i32 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i64(when));
                    super::super::super::wasi::io::poll::Pollable::from_handle(ret as u32)
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Create a `pollable` which will resolve once the given duration has
            /// elapsed, starting at the time at which this function was called.
            /// occured.
            pub fn subscribe_duration(when: Duration) -> Pollable {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:clocks/monotonic-clock@0.2.0")]
                    extern "C" {
                        #[link_name = "subscribe-duration"]
                        fn wit_import(_: i64) -> i32;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i64) -> i32 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i64(when));
                    super::super::super::wasi::io::poll::Pollable::from_handle(ret as u32)
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod http {
        #[allow(dead_code, clippy::all)]
        pub mod types {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type Duration = super::super::super::wasi::clocks::monotonic_clock::Duration;
            pub type InputStream = super::super::super::wasi::io::streams::InputStream;
            pub type OutputStream = super::super::super::wasi::io::streams::OutputStream;
            pub type IoError = super::super::super::wasi::io::error::Error;
            pub type Pollable = super::super::super::wasi::io::poll::Pollable;
            /// This type corresponds to HTTP standard Methods.
            #[derive(Clone)]
            pub enum Method {
                Get,
                Head,
                Post,
                Put,
                Delete,
                Connect,
                Options,
                Trace,
                Patch,
                Other(_rt::String),
            }
            impl ::core::fmt::Debug for Method {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        Method::Get => f.debug_tuple("Method::Get").finish(),
                        Method::Head => f.debug_tuple("Method::Head").finish(),
                        Method::Post => f.debug_tuple("Method::Post").finish(),
                        Method::Put => f.debug_tuple("Method::Put").finish(),
                        Method::Delete => f.debug_tuple("Method::Delete").finish(),
                        Method::Connect => f.debug_tuple("Method::Connect").finish(),
                        Method::Options => f.debug_tuple("Method::Options").finish(),
                        Method::Trace => f.debug_tuple("Method::Trace").finish(),
                        Method::Patch => f.debug_tuple("Method::Patch").finish(),
                        Method::Other(e) => f.debug_tuple("Method::Other").field(e).finish(),
                    }
                }
            }
            /// This type corresponds to HTTP standard Related Schemes.
            #[derive(Clone)]
            pub enum Scheme {
                Http,
                Https,
                Other(_rt::String),
            }
            impl ::core::fmt::Debug for Scheme {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        Scheme::Http => f.debug_tuple("Scheme::Http").finish(),
                        Scheme::Https => f.debug_tuple("Scheme::Https").finish(),
                        Scheme::Other(e) => f.debug_tuple("Scheme::Other").field(e).finish(),
                    }
                }
            }
            /// Defines the case payload type for `DNS-error` above:
            #[derive(Clone)]
            pub struct DnsErrorPayload {
                pub rcode: Option<_rt::String>,
                pub info_code: Option<u16>,
            }
            impl ::core::fmt::Debug for DnsErrorPayload {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("DnsErrorPayload")
                        .field("rcode", &self.rcode)
                        .field("info-code", &self.info_code)
                        .finish()
                }
            }
            /// Defines the case payload type for `TLS-alert-received` above:
            #[derive(Clone)]
            pub struct TlsAlertReceivedPayload {
                pub alert_id: Option<u8>,
                pub alert_message: Option<_rt::String>,
            }
            impl ::core::fmt::Debug for TlsAlertReceivedPayload {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("TlsAlertReceivedPayload")
                        .field("alert-id", &self.alert_id)
                        .field("alert-message", &self.alert_message)
                        .finish()
                }
            }
            /// Defines the case payload type for `HTTP-response-{header,trailer}-size` above:
            #[derive(Clone)]
            pub struct FieldSizePayload {
                pub field_name: Option<_rt::String>,
                pub field_size: Option<u32>,
            }
            impl ::core::fmt::Debug for FieldSizePayload {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("FieldSizePayload")
                        .field("field-name", &self.field_name)
                        .field("field-size", &self.field_size)
                        .finish()
                }
            }
            /// These cases are inspired by the IANA HTTP Proxy Error Types:
            /// https://www.iana.org/assignments/http-proxy-status/http-proxy-status.xhtml#table-http-proxy-error-types
            #[derive(Clone)]
            pub enum ErrorCode {
                DnsTimeout,
                DnsError(DnsErrorPayload),
                DestinationNotFound,
                DestinationUnavailable,
                DestinationIpProhibited,
                DestinationIpUnroutable,
                ConnectionRefused,
                ConnectionTerminated,
                ConnectionTimeout,
                ConnectionReadTimeout,
                ConnectionWriteTimeout,
                ConnectionLimitReached,
                TlsProtocolError,
                TlsCertificateError,
                TlsAlertReceived(TlsAlertReceivedPayload),
                HttpRequestDenied,
                HttpRequestLengthRequired,
                HttpRequestBodySize(Option<u64>),
                HttpRequestMethodInvalid,
                HttpRequestUriInvalid,
                HttpRequestUriTooLong,
                HttpRequestHeaderSectionSize(Option<u32>),
                HttpRequestHeaderSize(Option<FieldSizePayload>),
                HttpRequestTrailerSectionSize(Option<u32>),
                HttpRequestTrailerSize(FieldSizePayload),
                HttpResponseIncomplete,
                HttpResponseHeaderSectionSize(Option<u32>),
                HttpResponseHeaderSize(FieldSizePayload),
                HttpResponseBodySize(Option<u64>),
                HttpResponseTrailerSectionSize(Option<u32>),
                HttpResponseTrailerSize(FieldSizePayload),
                HttpResponseTransferCoding(Option<_rt::String>),
                HttpResponseContentCoding(Option<_rt::String>),
                HttpResponseTimeout,
                HttpUpgradeFailed,
                HttpProtocolError,
                LoopDetected,
                ConfigurationError,
                /// This is a catch-all error for anything that doesn't fit cleanly into a
                /// more specific case. It also includes an optional string for an
                /// unstructured description of the error. Users should not depend on the
                /// string for diagnosing errors, as it's not required to be consistent
                /// between implementations.
                InternalError(Option<_rt::String>),
            }
            impl ::core::fmt::Debug for ErrorCode {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        ErrorCode::DnsTimeout => f.debug_tuple("ErrorCode::DnsTimeout").finish(),
                        ErrorCode::DnsError(e) => {
                            f.debug_tuple("ErrorCode::DnsError").field(e).finish()
                        }
                        ErrorCode::DestinationNotFound => {
                            f.debug_tuple("ErrorCode::DestinationNotFound").finish()
                        }
                        ErrorCode::DestinationUnavailable => {
                            f.debug_tuple("ErrorCode::DestinationUnavailable").finish()
                        }
                        ErrorCode::DestinationIpProhibited => {
                            f.debug_tuple("ErrorCode::DestinationIpProhibited").finish()
                        }
                        ErrorCode::DestinationIpUnroutable => {
                            f.debug_tuple("ErrorCode::DestinationIpUnroutable").finish()
                        }
                        ErrorCode::ConnectionRefused => {
                            f.debug_tuple("ErrorCode::ConnectionRefused").finish()
                        }
                        ErrorCode::ConnectionTerminated => {
                            f.debug_tuple("ErrorCode::ConnectionTerminated").finish()
                        }
                        ErrorCode::ConnectionTimeout => {
                            f.debug_tuple("ErrorCode::ConnectionTimeout").finish()
                        }
                        ErrorCode::ConnectionReadTimeout => {
                            f.debug_tuple("ErrorCode::ConnectionReadTimeout").finish()
                        }
                        ErrorCode::ConnectionWriteTimeout => {
                            f.debug_tuple("ErrorCode::ConnectionWriteTimeout").finish()
                        }
                        ErrorCode::ConnectionLimitReached => {
                            f.debug_tuple("ErrorCode::ConnectionLimitReached").finish()
                        }
                        ErrorCode::TlsProtocolError => {
                            f.debug_tuple("ErrorCode::TlsProtocolError").finish()
                        }
                        ErrorCode::TlsCertificateError => {
                            f.debug_tuple("ErrorCode::TlsCertificateError").finish()
                        }
                        ErrorCode::TlsAlertReceived(e) => f
                            .debug_tuple("ErrorCode::TlsAlertReceived")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpRequestDenied => {
                            f.debug_tuple("ErrorCode::HttpRequestDenied").finish()
                        }
                        ErrorCode::HttpRequestLengthRequired => f
                            .debug_tuple("ErrorCode::HttpRequestLengthRequired")
                            .finish(),
                        ErrorCode::HttpRequestBodySize(e) => f
                            .debug_tuple("ErrorCode::HttpRequestBodySize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpRequestMethodInvalid => f
                            .debug_tuple("ErrorCode::HttpRequestMethodInvalid")
                            .finish(),
                        ErrorCode::HttpRequestUriInvalid => {
                            f.debug_tuple("ErrorCode::HttpRequestUriInvalid").finish()
                        }
                        ErrorCode::HttpRequestUriTooLong => {
                            f.debug_tuple("ErrorCode::HttpRequestUriTooLong").finish()
                        }
                        ErrorCode::HttpRequestHeaderSectionSize(e) => f
                            .debug_tuple("ErrorCode::HttpRequestHeaderSectionSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpRequestHeaderSize(e) => f
                            .debug_tuple("ErrorCode::HttpRequestHeaderSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpRequestTrailerSectionSize(e) => f
                            .debug_tuple("ErrorCode::HttpRequestTrailerSectionSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpRequestTrailerSize(e) => f
                            .debug_tuple("ErrorCode::HttpRequestTrailerSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseIncomplete => {
                            f.debug_tuple("ErrorCode::HttpResponseIncomplete").finish()
                        }
                        ErrorCode::HttpResponseHeaderSectionSize(e) => f
                            .debug_tuple("ErrorCode::HttpResponseHeaderSectionSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseHeaderSize(e) => f
                            .debug_tuple("ErrorCode::HttpResponseHeaderSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseBodySize(e) => f
                            .debug_tuple("ErrorCode::HttpResponseBodySize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseTrailerSectionSize(e) => f
                            .debug_tuple("ErrorCode::HttpResponseTrailerSectionSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseTrailerSize(e) => f
                            .debug_tuple("ErrorCode::HttpResponseTrailerSize")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseTransferCoding(e) => f
                            .debug_tuple("ErrorCode::HttpResponseTransferCoding")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseContentCoding(e) => f
                            .debug_tuple("ErrorCode::HttpResponseContentCoding")
                            .field(e)
                            .finish(),
                        ErrorCode::HttpResponseTimeout => {
                            f.debug_tuple("ErrorCode::HttpResponseTimeout").finish()
                        }
                        ErrorCode::HttpUpgradeFailed => {
                            f.debug_tuple("ErrorCode::HttpUpgradeFailed").finish()
                        }
                        ErrorCode::HttpProtocolError => {
                            f.debug_tuple("ErrorCode::HttpProtocolError").finish()
                        }
                        ErrorCode::LoopDetected => {
                            f.debug_tuple("ErrorCode::LoopDetected").finish()
                        }
                        ErrorCode::ConfigurationError => {
                            f.debug_tuple("ErrorCode::ConfigurationError").finish()
                        }
                        ErrorCode::InternalError(e) => {
                            f.debug_tuple("ErrorCode::InternalError").field(e).finish()
                        }
                    }
                }
            }
            impl ::core::fmt::Display for ErrorCode {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    write!(f, "{:?}", self)
                }
            }
            impl std::error::Error for ErrorCode {}
            /// This type enumerates the different kinds of errors that may occur when
            /// setting or appending to a `fields` resource.
            #[derive(Clone, Copy)]
            pub enum HeaderError {
                /// This error indicates that a `field-key` or `field-value` was
                /// syntactically invalid when used with an operation that sets headers in a
                /// `fields`.
                InvalidSyntax,
                /// This error indicates that a forbidden `field-key` was used when trying
                /// to set a header in a `fields`.
                Forbidden,
                /// This error indicates that the operation on the `fields` was not
                /// permitted because the fields are immutable.
                Immutable,
            }
            impl ::core::fmt::Debug for HeaderError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        HeaderError::InvalidSyntax => {
                            f.debug_tuple("HeaderError::InvalidSyntax").finish()
                        }
                        HeaderError::Forbidden => f.debug_tuple("HeaderError::Forbidden").finish(),
                        HeaderError::Immutable => f.debug_tuple("HeaderError::Immutable").finish(),
                    }
                }
            }
            impl ::core::fmt::Display for HeaderError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    write!(f, "{:?}", self)
                }
            }
            impl std::error::Error for HeaderError {}
            /// Field keys are always strings.
            pub type FieldKey = _rt::String;
            /// Field values should always be ASCII strings. However, in
            /// reality, HTTP implementations often have to interpret malformed values,
            /// so they are provided as a list of bytes.
            pub type FieldValue = _rt::Vec<u8>;
            /// This following block defines the `fields` resource which corresponds to
            /// HTTP standard Fields. Fields are a common representation used for both
            /// Headers and Trailers.
            ///
            /// A `fields` may be mutable or immutable. A `fields` created using the
            /// constructor, `from-list`, or `clone` will be mutable, but a `fields`
            /// resource given by other means (including, but not limited to,
            /// `incoming-request.headers`, `outgoing-request.headers`) might be be
            /// immutable. In an immutable fields, the `set`, `append`, and `delete`
            /// operations will fail with `header-error.immutable`.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct Fields {
                handle: _rt::Resource<Fields>,
            }
            impl Fields {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for Fields {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]fields"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Headers is an alias for Fields.
            pub type Headers = Fields;
            /// Trailers is an alias for Fields.
            pub type Trailers = Fields;
            /// Represents an incoming HTTP Request.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct IncomingRequest {
                handle: _rt::Resource<IncomingRequest>,
            }
            impl IncomingRequest {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for IncomingRequest {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]incoming-request"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents an outgoing HTTP Request.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct OutgoingRequest {
                handle: _rt::Resource<OutgoingRequest>,
            }
            impl OutgoingRequest {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for OutgoingRequest {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]outgoing-request"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Parameters for making an HTTP Request. Each of these parameters is
            /// currently an optional timeout applicable to the transport layer of the
            /// HTTP protocol.
            ///
            /// These timeouts are separate from any the user may use to bound a
            /// blocking call to `wasi:io/poll.poll`.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct RequestOptions {
                handle: _rt::Resource<RequestOptions>,
            }
            impl RequestOptions {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for RequestOptions {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]request-options"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents the ability to send an HTTP Response.
            ///
            /// This resource is used by the `wasi:http/incoming-handler` interface to
            /// allow a Response to be sent corresponding to the Request provided as the
            /// other argument to `incoming-handler.handle`.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct ResponseOutparam {
                handle: _rt::Resource<ResponseOutparam>,
            }
            impl ResponseOutparam {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for ResponseOutparam {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]response-outparam"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// This type corresponds to the HTTP standard Status Code.
            pub type StatusCode = u16;
            /// Represents an incoming HTTP Response.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct IncomingResponse {
                handle: _rt::Resource<IncomingResponse>,
            }
            impl IncomingResponse {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for IncomingResponse {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]incoming-response"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents an incoming HTTP Request or Response's Body.
            ///
            /// A body has both its contents - a stream of bytes - and a (possibly
            /// empty) set of trailers, indicating that the full contents of the
            /// body have been received. This resource represents the contents as
            /// an `input-stream` and the delivery of trailers as a `future-trailers`,
            /// and ensures that the user of this interface may only be consuming either
            /// the body contents or waiting on trailers at any given time.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct IncomingBody {
                handle: _rt::Resource<IncomingBody>,
            }
            impl IncomingBody {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for IncomingBody {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]incoming-body"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents a future which may eventaully return trailers, or an error.
            ///
            /// In the case that the incoming HTTP Request or Response did not have any
            /// trailers, this future will resolve to the empty set of trailers once the
            /// complete Request or Response body has been received.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct FutureTrailers {
                handle: _rt::Resource<FutureTrailers>,
            }
            impl FutureTrailers {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for FutureTrailers {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]future-trailers"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents an outgoing HTTP Response.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct OutgoingResponse {
                handle: _rt::Resource<OutgoingResponse>,
            }
            impl OutgoingResponse {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for OutgoingResponse {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]outgoing-response"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents an outgoing HTTP Request or Response's Body.
            ///
            /// A body has both its contents - a stream of bytes - and a (possibly
            /// empty) set of trailers, inducating the full contents of the body
            /// have been sent. This resource represents the contents as an
            /// `output-stream` child resource, and the completion of the body (with
            /// optional trailers) with a static function that consumes the
            /// `outgoing-body` resource, and ensures that the user of this interface
            /// may not write to the body contents after the body has been finished.
            ///
            /// If the user code drops this resource, as opposed to calling the static
            /// method `finish`, the implementation should treat the body as incomplete,
            /// and that an error has occured. The implementation should propogate this
            /// error to the HTTP protocol by whatever means it has available,
            /// including: corrupting the body on the wire, aborting the associated
            /// Request, or sending a late status code for the Response.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct OutgoingBody {
                handle: _rt::Resource<OutgoingBody>,
            }
            impl OutgoingBody {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for OutgoingBody {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]outgoing-body"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// Represents a future which may eventaully return an incoming HTTP
            /// Response, or an error.
            ///
            /// This resource is returned by the `wasi:http/outgoing-handler` interface to
            /// provide the HTTP Response corresponding to the sent Request.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct FutureIncomingResponse {
                handle: _rt::Resource<FutureIncomingResponse>,
            }
            impl FutureIncomingResponse {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for FutureIncomingResponse {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]future-incoming-response"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Attempts to extract a http-related `error` from the wasi:io `error`
            /// provided.
            ///
            /// Stream operations which return
            /// `wasi:io/stream/stream-error::last-operation-failed` have a payload of
            /// type `wasi:io/error/error` with more information about the operation
            /// that failed. This payload can be passed through to this function to see
            /// if there's http-related information about the error to return.
            ///
            /// Note that this function is fallible because not all io-errors are
            /// http-related errors.
            pub fn http_error_code(err: &IoError) -> Option<ErrorCode> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 40]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 40]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                    extern "C" {
                        #[link_name = "http-error-code"]
                        fn wit_import(_: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import((err).handle() as i32, ptr0);
                    let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                    match l1 {
                        0 => None,
                        1 => {
                            let e = {
                                let l2 = i32::from(*ptr0.add(8).cast::<u8>());
                                let v64 = match l2 {
                                    0 => ErrorCode::DnsTimeout,
                                    1 => {
                                        let e64 = {
                                            let l3 = i32::from(*ptr0.add(16).cast::<u8>());
                                            let l7 = i32::from(*ptr0.add(28).cast::<u8>());
                                            DnsErrorPayload {
                                                rcode: match l3 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l4 =
                                                                *ptr0.add(20).cast::<*mut u8>();
                                                            let l5 = *ptr0.add(24).cast::<usize>();
                                                            let len6 = l5;
                                                            let bytes6 = _rt::Vec::from_raw_parts(
                                                                l4.cast(),
                                                                len6,
                                                                len6,
                                                            );
                                                            _rt::string_lift(bytes6)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                                info_code: match l7 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l8 = i32::from(
                                                                *ptr0.add(30).cast::<u16>(),
                                                            );
                                                            l8 as u16
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                            }
                                        };
                                        ErrorCode::DnsError(e64)
                                    }
                                    2 => ErrorCode::DestinationNotFound,
                                    3 => ErrorCode::DestinationUnavailable,
                                    4 => ErrorCode::DestinationIpProhibited,
                                    5 => ErrorCode::DestinationIpUnroutable,
                                    6 => ErrorCode::ConnectionRefused,
                                    7 => ErrorCode::ConnectionTerminated,
                                    8 => ErrorCode::ConnectionTimeout,
                                    9 => ErrorCode::ConnectionReadTimeout,
                                    10 => ErrorCode::ConnectionWriteTimeout,
                                    11 => ErrorCode::ConnectionLimitReached,
                                    12 => ErrorCode::TlsProtocolError,
                                    13 => ErrorCode::TlsCertificateError,
                                    14 => {
                                        let e64 = {
                                            let l9 = i32::from(*ptr0.add(16).cast::<u8>());
                                            let l11 = i32::from(*ptr0.add(20).cast::<u8>());
                                            TlsAlertReceivedPayload {
                                                alert_id: match l9 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l10 = i32::from(
                                                                *ptr0.add(17).cast::<u8>(),
                                                            );
                                                            l10 as u8
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                                alert_message: match l11 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l12 =
                                                                *ptr0.add(24).cast::<*mut u8>();
                                                            let l13 = *ptr0.add(28).cast::<usize>();
                                                            let len14 = l13;
                                                            let bytes14 = _rt::Vec::from_raw_parts(
                                                                l12.cast(),
                                                                len14,
                                                                len14,
                                                            );
                                                            _rt::string_lift(bytes14)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                            }
                                        };
                                        ErrorCode::TlsAlertReceived(e64)
                                    }
                                    15 => ErrorCode::HttpRequestDenied,
                                    16 => ErrorCode::HttpRequestLengthRequired,
                                    17 => {
                                        let e64 = {
                                            let l15 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l15 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l16 = *ptr0.add(24).cast::<i64>();
                                                        l16 as u64
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpRequestBodySize(e64)
                                    }
                                    18 => ErrorCode::HttpRequestMethodInvalid,
                                    19 => ErrorCode::HttpRequestUriInvalid,
                                    20 => ErrorCode::HttpRequestUriTooLong,
                                    21 => {
                                        let e64 = {
                                            let l17 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l17 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l18 = *ptr0.add(20).cast::<i32>();
                                                        l18 as u32
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpRequestHeaderSectionSize(e64)
                                    }
                                    22 => {
                                        let e64 = {
                                            let l19 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l19 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l20 =
                                                            i32::from(*ptr0.add(20).cast::<u8>());
                                                        let l24 =
                                                            i32::from(*ptr0.add(32).cast::<u8>());
                                                        FieldSizePayload {
                                                            field_name: match l20 {
                                                                0 => None,
                                                                1 => {
                                                                    let e = {
                                                                        let l21 = *ptr0
                                                                            .add(24)
                                                                            .cast::<*mut u8>(
                                                                        );
                                                                        let l22 = *ptr0
                                                                            .add(28)
                                                                            .cast::<usize>();
                                                                        let len23 = l22;
                                                                        let bytes23 = _rt::Vec::from_raw_parts(
                                                                            l21.cast(),
                                                                            len23,
                                                                            len23,
                                                                        );
                                                                        _rt::string_lift(bytes23)
                                                                    };
                                                                    Some(e)
                                                                }
                                                                _ => {
                                                                    _rt::invalid_enum_discriminant()
                                                                }
                                                            },
                                                            field_size: match l24 {
                                                                0 => None,
                                                                1 => {
                                                                    let e = {
                                                                        let l25 = *ptr0
                                                                            .add(36)
                                                                            .cast::<i32>();
                                                                        l25 as u32
                                                                    };
                                                                    Some(e)
                                                                }
                                                                _ => {
                                                                    _rt::invalid_enum_discriminant()
                                                                }
                                                            },
                                                        }
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpRequestHeaderSize(e64)
                                    }
                                    23 => {
                                        let e64 = {
                                            let l26 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l26 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l27 = *ptr0.add(20).cast::<i32>();
                                                        l27 as u32
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpRequestTrailerSectionSize(e64)
                                    }
                                    24 => {
                                        let e64 = {
                                            let l28 = i32::from(*ptr0.add(16).cast::<u8>());
                                            let l32 = i32::from(*ptr0.add(28).cast::<u8>());
                                            FieldSizePayload {
                                                field_name: match l28 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l29 =
                                                                *ptr0.add(20).cast::<*mut u8>();
                                                            let l30 = *ptr0.add(24).cast::<usize>();
                                                            let len31 = l30;
                                                            let bytes31 = _rt::Vec::from_raw_parts(
                                                                l29.cast(),
                                                                len31,
                                                                len31,
                                                            );
                                                            _rt::string_lift(bytes31)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                                field_size: match l32 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l33 = *ptr0.add(32).cast::<i32>();
                                                            l33 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                            }
                                        };
                                        ErrorCode::HttpRequestTrailerSize(e64)
                                    }
                                    25 => ErrorCode::HttpResponseIncomplete,
                                    26 => {
                                        let e64 = {
                                            let l34 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l34 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l35 = *ptr0.add(20).cast::<i32>();
                                                        l35 as u32
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpResponseHeaderSectionSize(e64)
                                    }
                                    27 => {
                                        let e64 = {
                                            let l36 = i32::from(*ptr0.add(16).cast::<u8>());
                                            let l40 = i32::from(*ptr0.add(28).cast::<u8>());
                                            FieldSizePayload {
                                                field_name: match l36 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l37 =
                                                                *ptr0.add(20).cast::<*mut u8>();
                                                            let l38 = *ptr0.add(24).cast::<usize>();
                                                            let len39 = l38;
                                                            let bytes39 = _rt::Vec::from_raw_parts(
                                                                l37.cast(),
                                                                len39,
                                                                len39,
                                                            );
                                                            _rt::string_lift(bytes39)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                                field_size: match l40 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l41 = *ptr0.add(32).cast::<i32>();
                                                            l41 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                            }
                                        };
                                        ErrorCode::HttpResponseHeaderSize(e64)
                                    }
                                    28 => {
                                        let e64 = {
                                            let l42 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l42 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l43 = *ptr0.add(24).cast::<i64>();
                                                        l43 as u64
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpResponseBodySize(e64)
                                    }
                                    29 => {
                                        let e64 = {
                                            let l44 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l44 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l45 = *ptr0.add(20).cast::<i32>();
                                                        l45 as u32
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpResponseTrailerSectionSize(e64)
                                    }
                                    30 => {
                                        let e64 = {
                                            let l46 = i32::from(*ptr0.add(16).cast::<u8>());
                                            let l50 = i32::from(*ptr0.add(28).cast::<u8>());
                                            FieldSizePayload {
                                                field_name: match l46 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l47 =
                                                                *ptr0.add(20).cast::<*mut u8>();
                                                            let l48 = *ptr0.add(24).cast::<usize>();
                                                            let len49 = l48;
                                                            let bytes49 = _rt::Vec::from_raw_parts(
                                                                l47.cast(),
                                                                len49,
                                                                len49,
                                                            );
                                                            _rt::string_lift(bytes49)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                                field_size: match l50 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l51 = *ptr0.add(32).cast::<i32>();
                                                            l51 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                },
                                            }
                                        };
                                        ErrorCode::HttpResponseTrailerSize(e64)
                                    }
                                    31 => {
                                        let e64 = {
                                            let l52 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l52 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l53 = *ptr0.add(20).cast::<*mut u8>();
                                                        let l54 = *ptr0.add(24).cast::<usize>();
                                                        let len55 = l54;
                                                        let bytes55 = _rt::Vec::from_raw_parts(
                                                            l53.cast(),
                                                            len55,
                                                            len55,
                                                        );
                                                        _rt::string_lift(bytes55)
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpResponseTransferCoding(e64)
                                    }
                                    32 => {
                                        let e64 = {
                                            let l56 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l56 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l57 = *ptr0.add(20).cast::<*mut u8>();
                                                        let l58 = *ptr0.add(24).cast::<usize>();
                                                        let len59 = l58;
                                                        let bytes59 = _rt::Vec::from_raw_parts(
                                                            l57.cast(),
                                                            len59,
                                                            len59,
                                                        );
                                                        _rt::string_lift(bytes59)
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::HttpResponseContentCoding(e64)
                                    }
                                    33 => ErrorCode::HttpResponseTimeout,
                                    34 => ErrorCode::HttpUpgradeFailed,
                                    35 => ErrorCode::HttpProtocolError,
                                    36 => ErrorCode::LoopDetected,
                                    37 => ErrorCode::ConfigurationError,
                                    n => {
                                        debug_assert_eq!(n, 38, "invalid enum discriminant");
                                        let e64 = {
                                            let l60 = i32::from(*ptr0.add(16).cast::<u8>());
                                            match l60 {
                                                0 => None,
                                                1 => {
                                                    let e = {
                                                        let l61 = *ptr0.add(20).cast::<*mut u8>();
                                                        let l62 = *ptr0.add(24).cast::<usize>();
                                                        let len63 = l62;
                                                        let bytes63 = _rt::Vec::from_raw_parts(
                                                            l61.cast(),
                                                            len63,
                                                            len63,
                                                        );
                                                        _rt::string_lift(bytes63)
                                                    };
                                                    Some(e)
                                                }
                                                _ => _rt::invalid_enum_discriminant(),
                                            }
                                        };
                                        ErrorCode::InternalError(e64)
                                    }
                                };
                                v64
                            };
                            Some(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Construct an empty HTTP Fields.
                ///
                /// The resulting `fields` is mutable.
                pub fn new() -> Self {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[constructor]fields"]
                            fn wit_import() -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import() -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import();
                        Fields::from_handle(ret as u32)
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Construct an HTTP Fields.
                ///
                /// The resulting `fields` is mutable.
                ///
                /// The list represents each key-value pair in the Fields. Keys
                /// which have multiple values are represented by multiple entries in this
                /// list with the same key.
                ///
                /// The tuple is a pair of the field key, represented as a string, and
                /// Value, represented as a list of bytes. In a valid Fields, all keys
                /// and values are valid UTF-8 strings. However, values are not always
                /// well-formed, so they are represented as a raw list of bytes.
                ///
                /// An error result will be returned if any header or value was
                /// syntactically invalid, or if a header was forbidden.
                pub fn from_list(
                    entries: &[(FieldKey, FieldValue)],
                ) -> Result<Fields, HeaderError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let vec3 = entries;
                        let len3 = vec3.len();
                        let layout3 =
                            _rt::alloc::Layout::from_size_align_unchecked(vec3.len() * 16, 4);
                        let result3 = if layout3.size() != 0 {
                            let ptr = _rt::alloc::alloc(layout3).cast::<u8>();
                            if ptr.is_null() {
                                _rt::alloc::handle_alloc_error(layout3);
                            }
                            ptr
                        } else {
                            {
                                ::core::ptr::null_mut()
                            }
                        };
                        for (i, e) in vec3.into_iter().enumerate() {
                            let base = result3.add(i * 16);
                            {
                                let (t0_0, t0_1) = e;
                                let vec1 = t0_0;
                                let ptr1 = vec1.as_ptr().cast::<u8>();
                                let len1 = vec1.len();
                                *base.add(4).cast::<usize>() = len1;
                                *base.add(0).cast::<*mut u8>() = ptr1.cast_mut();
                                let vec2 = t0_1;
                                let ptr2 = vec2.as_ptr().cast::<u8>();
                                let len2 = vec2.len();
                                *base.add(12).cast::<usize>() = len2;
                                *base.add(8).cast::<*mut u8>() = ptr2.cast_mut();
                            }
                        }
                        let ptr4 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[static]fields.from-list"]
                            fn wit_import(_: *mut u8, _: usize, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import(result3, len3, ptr4);
                        let l5 = i32::from(*ptr4.add(0).cast::<u8>());
                        if layout3.size() != 0 {
                            _rt::alloc::dealloc(result3.cast(), layout3);
                        }
                        match l5 {
                            0 => {
                                let e = {
                                    let l6 = *ptr4.add(4).cast::<i32>();
                                    Fields::from_handle(l6 as u32)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l7 = i32::from(*ptr4.add(4).cast::<u8>());
                                    let v8 = match l7 {
                                        0 => HeaderError::InvalidSyntax,
                                        1 => HeaderError::Forbidden,
                                        n => {
                                            debug_assert_eq!(n, 2, "invalid enum discriminant");
                                            HeaderError::Immutable
                                        }
                                    };
                                    v8
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Get all of the values corresponding to a key. If the key is not present
                /// in this `fields`, an empty list is returned. However, if the key is
                /// present but empty, this is represented by a list with one or more
                /// empty field-values present.
                pub fn get(&self, name: &FieldKey) -> _rt::Vec<FieldValue> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let vec0 = name;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.get"]
                            fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0.cast_mut(), len0, ptr1);
                        let l2 = *ptr1.add(0).cast::<*mut u8>();
                        let l3 = *ptr1.add(4).cast::<usize>();
                        let base7 = l2;
                        let len7 = l3;
                        let mut result7 = _rt::Vec::with_capacity(len7);
                        for i in 0..len7 {
                            let base = base7.add(i * 8);
                            let e7 = {
                                let l4 = *base.add(0).cast::<*mut u8>();
                                let l5 = *base.add(4).cast::<usize>();
                                let len6 = l5;
                                _rt::Vec::from_raw_parts(l4.cast(), len6, len6)
                            };
                            result7.push(e7);
                        }
                        _rt::cabi_dealloc(base7, len7 * 8, 4);
                        result7
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns `true` when the key is present in this `fields`. If the key is
                /// syntactically invalid, `false` is returned.
                pub fn has(&self, name: &FieldKey) -> bool {
                    unsafe {
                        let vec0 = name;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.has"]
                            fn wit_import(_: i32, _: *mut u8, _: usize) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8, _: usize) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32, ptr0.cast_mut(), len0);
                        _rt::bool_lift(ret as u8)
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Set all of the values for a key. Clears any existing values for that
                /// key, if they have been set.
                ///
                /// Fails with `header-error.immutable` if the `fields` are immutable.
                pub fn set(
                    &self,
                    name: &FieldKey,
                    value: &[FieldValue],
                ) -> Result<(), HeaderError> {
                    unsafe {
                        #[repr(align(1))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 2]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 2]);
                        let vec0 = name;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        let vec2 = value;
                        let len2 = vec2.len();
                        let layout2 =
                            _rt::alloc::Layout::from_size_align_unchecked(vec2.len() * 8, 4);
                        let result2 = if layout2.size() != 0 {
                            let ptr = _rt::alloc::alloc(layout2).cast::<u8>();
                            if ptr.is_null() {
                                _rt::alloc::handle_alloc_error(layout2);
                            }
                            ptr
                        } else {
                            {
                                ::core::ptr::null_mut()
                            }
                        };
                        for (i, e) in vec2.into_iter().enumerate() {
                            let base = result2.add(i * 8);
                            {
                                let vec1 = e;
                                let ptr1 = vec1.as_ptr().cast::<u8>();
                                let len1 = vec1.len();
                                *base.add(4).cast::<usize>() = len1;
                                *base.add(0).cast::<*mut u8>() = ptr1.cast_mut();
                            }
                        }
                        let ptr3 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.set"]
                            fn wit_import(
                                _: i32,
                                _: *mut u8,
                                _: usize,
                                _: *mut u8,
                                _: usize,
                                _: *mut u8,
                            );
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(
                            _: i32,
                            _: *mut u8,
                            _: usize,
                            _: *mut u8,
                            _: usize,
                            _: *mut u8,
                        ) {
                            unreachable!()
                        }
                        wit_import(
                            (self).handle() as i32,
                            ptr0.cast_mut(),
                            len0,
                            result2,
                            len2,
                            ptr3,
                        );
                        let l4 = i32::from(*ptr3.add(0).cast::<u8>());
                        if layout2.size() != 0 {
                            _rt::alloc::dealloc(result2.cast(), layout2);
                        }
                        match l4 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l5 = i32::from(*ptr3.add(1).cast::<u8>());
                                    let v6 = match l5 {
                                        0 => HeaderError::InvalidSyntax,
                                        1 => HeaderError::Forbidden,
                                        n => {
                                            debug_assert_eq!(n, 2, "invalid enum discriminant");
                                            HeaderError::Immutable
                                        }
                                    };
                                    v6
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Delete all values for a key. Does nothing if no values for the key
                /// exist.
                ///
                /// Fails with `header-error.immutable` if the `fields` are immutable.
                pub fn delete(&self, name: &FieldKey) -> Result<(), HeaderError> {
                    unsafe {
                        #[repr(align(1))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 2]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 2]);
                        let vec0 = name;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.delete"]
                            fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0.cast_mut(), len0, ptr1);
                        let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                        match l2 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr1.add(1).cast::<u8>());
                                    let v4 = match l3 {
                                        0 => HeaderError::InvalidSyntax,
                                        1 => HeaderError::Forbidden,
                                        n => {
                                            debug_assert_eq!(n, 2, "invalid enum discriminant");
                                            HeaderError::Immutable
                                        }
                                    };
                                    v4
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Append a value for a key. Does not change or delete any existing
                /// values for that key.
                ///
                /// Fails with `header-error.immutable` if the `fields` are immutable.
                pub fn append(
                    &self,
                    name: &FieldKey,
                    value: &FieldValue,
                ) -> Result<(), HeaderError> {
                    unsafe {
                        #[repr(align(1))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 2]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 2]);
                        let vec0 = name;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        let vec1 = value;
                        let ptr1 = vec1.as_ptr().cast::<u8>();
                        let len1 = vec1.len();
                        let ptr2 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.append"]
                            fn wit_import(
                                _: i32,
                                _: *mut u8,
                                _: usize,
                                _: *mut u8,
                                _: usize,
                                _: *mut u8,
                            );
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(
                            _: i32,
                            _: *mut u8,
                            _: usize,
                            _: *mut u8,
                            _: usize,
                            _: *mut u8,
                        ) {
                            unreachable!()
                        }
                        wit_import(
                            (self).handle() as i32,
                            ptr0.cast_mut(),
                            len0,
                            ptr1.cast_mut(),
                            len1,
                            ptr2,
                        );
                        let l3 = i32::from(*ptr2.add(0).cast::<u8>());
                        match l3 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l4 = i32::from(*ptr2.add(1).cast::<u8>());
                                    let v5 = match l4 {
                                        0 => HeaderError::InvalidSyntax,
                                        1 => HeaderError::Forbidden,
                                        n => {
                                            debug_assert_eq!(n, 2, "invalid enum discriminant");
                                            HeaderError::Immutable
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Retrieve the full set of keys and values in the Fields. Like the
                /// constructor, the list represents each key-value pair.
                ///
                /// The outer list represents each key-value pair in the Fields. Keys
                /// which have multiple values are represented by multiple entries in this
                /// list with the same key.
                pub fn entries(&self) -> _rt::Vec<(FieldKey, FieldValue)> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.entries"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = *ptr0.add(0).cast::<*mut u8>();
                        let l2 = *ptr0.add(4).cast::<usize>();
                        let base9 = l1;
                        let len9 = l2;
                        let mut result9 = _rt::Vec::with_capacity(len9);
                        for i in 0..len9 {
                            let base = base9.add(i * 16);
                            let e9 = {
                                let l3 = *base.add(0).cast::<*mut u8>();
                                let l4 = *base.add(4).cast::<usize>();
                                let len5 = l4;
                                let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                let l6 = *base.add(8).cast::<*mut u8>();
                                let l7 = *base.add(12).cast::<usize>();
                                let len8 = l7;
                                (
                                    _rt::string_lift(bytes5),
                                    _rt::Vec::from_raw_parts(l6.cast(), len8, len8),
                                )
                            };
                            result9.push(e9);
                        }
                        _rt::cabi_dealloc(base9, len9 * 16, 4);
                        result9
                    }
                }
            }
            impl Fields {
                #[allow(unused_unsafe, clippy::all)]
                /// Make a deep copy of the Fields. Equivelant in behavior to calling the
                /// `fields` constructor on the return value of `entries`. The resulting
                /// `fields` is mutable.
                pub fn clone(&self) -> Fields {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]fields.clone"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        Fields::from_handle(ret as u32)
                    }
                }
            }
            impl IncomingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the method of the incoming request.
                pub fn method(&self) -> Method {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-request.method"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        let v5 = match l1 {
                            0 => Method::Get,
                            1 => Method::Head,
                            2 => Method::Post,
                            3 => Method::Put,
                            4 => Method::Delete,
                            5 => Method::Connect,
                            6 => Method::Options,
                            7 => Method::Trace,
                            8 => Method::Patch,
                            n => {
                                debug_assert_eq!(n, 9, "invalid enum discriminant");
                                let e5 = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                    _rt::string_lift(bytes4)
                                };
                                Method::Other(e5)
                            }
                        };
                        v5
                    }
                }
            }
            impl IncomingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the path with query parameters from the request, as a string.
                pub fn path_with_query(&self) -> Option<_rt::String> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-request.path-with-query"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                    _rt::string_lift(bytes4)
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl IncomingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the protocol scheme from the request.
                pub fn scheme(&self) -> Option<Scheme> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-request.scheme"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v6 = match l2 {
                                        0 => Scheme::Http,
                                        1 => Scheme::Https,
                                        n => {
                                            debug_assert_eq!(n, 2, "invalid enum discriminant");
                                            let e6 = {
                                                let l3 = *ptr0.add(8).cast::<*mut u8>();
                                                let l4 = *ptr0.add(12).cast::<usize>();
                                                let len5 = l4;
                                                let bytes5 =
                                                    _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                                _rt::string_lift(bytes5)
                                            };
                                            Scheme::Other(e6)
                                        }
                                    };
                                    v6
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl IncomingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the authority from the request, if it was present.
                pub fn authority(&self) -> Option<_rt::String> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-request.authority"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                    _rt::string_lift(bytes4)
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl IncomingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the `headers` associated with the request.
                ///
                /// The returned `headers` resource is immutable: `set`, `append`, and
                /// `delete` operations will fail with `header-error.immutable`.
                ///
                /// The `headers` returned are a child resource: it must be dropped before
                /// the parent `incoming-request` is dropped. Dropping this
                /// `incoming-request` before all children are dropped will trap.
                pub fn headers(&self) -> Headers {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-request.headers"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        Fields::from_handle(ret as u32)
                    }
                }
            }
            impl IncomingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Gives the `incoming-body` associated with this request. Will only
                /// return success at most once, and subsequent calls will return error.
                pub fn consume(&self) -> Result<IncomingBody, ()> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-request.consume"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<i32>();
                                    IncomingBody::from_handle(l2 as u32)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Construct a new `outgoing-request` with a default `method` of `GET`, and
                /// `none` values for `path-with-query`, `scheme`, and `authority`.
                ///
                /// * `headers` is the HTTP Headers for the Request.
                ///
                /// It is possible to construct, or manipulate with the accessor functions
                /// below, an `outgoing-request` with an invalid combination of `scheme`
                /// and `authority`, or `headers` which are not permitted to be sent.
                /// It is the obligation of the `outgoing-handler.handle` implementation
                /// to reject invalid constructions of `outgoing-request`.
                pub fn new(headers: Headers) -> Self {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[constructor]outgoing-request"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((&headers).take_handle() as i32);
                        OutgoingRequest::from_handle(ret as u32)
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the resource corresponding to the outgoing Body for this
                /// Request.
                ///
                /// Returns success on the first call: the `outgoing-body` resource for
                /// this `outgoing-request` can be retrieved at most once. Subsequent
                /// calls will return error.
                pub fn body(&self) -> Result<OutgoingBody, ()> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.body"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<i32>();
                                    OutgoingBody::from_handle(l2 as u32)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the Method for the Request.
                pub fn method(&self) -> Method {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.method"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        let v5 = match l1 {
                            0 => Method::Get,
                            1 => Method::Head,
                            2 => Method::Post,
                            3 => Method::Put,
                            4 => Method::Delete,
                            5 => Method::Connect,
                            6 => Method::Options,
                            7 => Method::Trace,
                            8 => Method::Patch,
                            n => {
                                debug_assert_eq!(n, 9, "invalid enum discriminant");
                                let e5 = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                    _rt::string_lift(bytes4)
                                };
                                Method::Other(e5)
                            }
                        };
                        v5
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the Method for the Request. Fails if the string present in a
                /// `method.other` argument is not a syntactically valid method.
                pub fn set_method(&self, method: &Method) -> Result<(), ()> {
                    unsafe {
                        let (result1_0, result1_1, result1_2) = match method {
                            Method::Get => (0i32, ::core::ptr::null_mut(), 0usize),
                            Method::Head => (1i32, ::core::ptr::null_mut(), 0usize),
                            Method::Post => (2i32, ::core::ptr::null_mut(), 0usize),
                            Method::Put => (3i32, ::core::ptr::null_mut(), 0usize),
                            Method::Delete => (4i32, ::core::ptr::null_mut(), 0usize),
                            Method::Connect => (5i32, ::core::ptr::null_mut(), 0usize),
                            Method::Options => (6i32, ::core::ptr::null_mut(), 0usize),
                            Method::Trace => (7i32, ::core::ptr::null_mut(), 0usize),
                            Method::Patch => (8i32, ::core::ptr::null_mut(), 0usize),
                            Method::Other(e) => {
                                let vec0 = e;
                                let ptr0 = vec0.as_ptr().cast::<u8>();
                                let len0 = vec0.len();
                                (9i32, ptr0.cast_mut(), len0)
                            }
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.set-method"]
                            fn wit_import(_: i32, _: i32, _: *mut u8, _: usize) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: *mut u8, _: usize) -> i32 {
                            unreachable!()
                        }
                        let ret =
                            wit_import((self).handle() as i32, result1_0, result1_1, result1_2);
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the combination of the HTTP Path and Query for the Request.
                /// When `none`, this represents an empty Path and empty Query.
                pub fn path_with_query(&self) -> Option<_rt::String> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.path-with-query"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                    _rt::string_lift(bytes4)
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the combination of the HTTP Path and Query for the Request.
                /// When `none`, this represents an empty Path and empty Query. Fails is the
                /// string given is not a syntactically valid path and query uri component.
                pub fn set_path_with_query(&self, path_with_query: Option<&str>) -> Result<(), ()> {
                    unsafe {
                        let (result1_0, result1_1, result1_2) = match path_with_query {
                            Some(e) => {
                                let vec0 = e;
                                let ptr0 = vec0.as_ptr().cast::<u8>();
                                let len0 = vec0.len();
                                (1i32, ptr0.cast_mut(), len0)
                            }
                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.set-path-with-query"]
                            fn wit_import(_: i32, _: i32, _: *mut u8, _: usize) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: *mut u8, _: usize) -> i32 {
                            unreachable!()
                        }
                        let ret =
                            wit_import((self).handle() as i32, result1_0, result1_1, result1_2);
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the HTTP Related Scheme for the Request. When `none`, the
                /// implementation may choose an appropriate default scheme.
                pub fn scheme(&self) -> Option<Scheme> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.scheme"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v6 = match l2 {
                                        0 => Scheme::Http,
                                        1 => Scheme::Https,
                                        n => {
                                            debug_assert_eq!(n, 2, "invalid enum discriminant");
                                            let e6 = {
                                                let l3 = *ptr0.add(8).cast::<*mut u8>();
                                                let l4 = *ptr0.add(12).cast::<usize>();
                                                let len5 = l4;
                                                let bytes5 =
                                                    _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                                _rt::string_lift(bytes5)
                                            };
                                            Scheme::Other(e6)
                                        }
                                    };
                                    v6
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the HTTP Related Scheme for the Request. When `none`, the
                /// implementation may choose an appropriate default scheme. Fails if the
                /// string given is not a syntactically valid uri scheme.
                pub fn set_scheme(&self, scheme: Option<&Scheme>) -> Result<(), ()> {
                    unsafe {
                        let (result2_0, result2_1, result2_2, result2_3) = match scheme {
                            Some(e) => {
                                let (result1_0, result1_1, result1_2) = match e {
                                    Scheme::Http => (0i32, ::core::ptr::null_mut(), 0usize),
                                    Scheme::Https => (1i32, ::core::ptr::null_mut(), 0usize),
                                    Scheme::Other(e) => {
                                        let vec0 = e;
                                        let ptr0 = vec0.as_ptr().cast::<u8>();
                                        let len0 = vec0.len();
                                        (2i32, ptr0.cast_mut(), len0)
                                    }
                                };
                                (1i32, result1_0, result1_1, result1_2)
                            }
                            None => (0i32, 0i32, ::core::ptr::null_mut(), 0usize),
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.set-scheme"]
                            fn wit_import(_: i32, _: i32, _: i32, _: *mut u8, _: usize) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i32, _: *mut u8, _: usize) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import(
                            (self).handle() as i32,
                            result2_0,
                            result2_1,
                            result2_2,
                            result2_3,
                        );
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the HTTP Authority for the Request. A value of `none` may be used
                /// with Related Schemes which do not require an Authority. The HTTP and
                /// HTTPS schemes always require an authority.
                pub fn authority(&self) -> Option<_rt::String> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.authority"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                    _rt::string_lift(bytes4)
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the HTTP Authority for the Request. A value of `none` may be used
                /// with Related Schemes which do not require an Authority. The HTTP and
                /// HTTPS schemes always require an authority. Fails if the string given is
                /// not a syntactically valid uri authority.
                pub fn set_authority(&self, authority: Option<&str>) -> Result<(), ()> {
                    unsafe {
                        let (result1_0, result1_1, result1_2) = match authority {
                            Some(e) => {
                                let vec0 = e;
                                let ptr0 = vec0.as_ptr().cast::<u8>();
                                let len0 = vec0.len();
                                (1i32, ptr0.cast_mut(), len0)
                            }
                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.set-authority"]
                            fn wit_import(_: i32, _: i32, _: *mut u8, _: usize) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: *mut u8, _: usize) -> i32 {
                            unreachable!()
                        }
                        let ret =
                            wit_import((self).handle() as i32, result1_0, result1_1, result1_2);
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingRequest {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the headers associated with the Request.
                ///
                /// The returned `headers` resource is immutable: `set`, `append`, and
                /// `delete` operations will fail with `header-error.immutable`.
                ///
                /// This headers resource is a child: it must be dropped before the parent
                /// `outgoing-request` is dropped, or its ownership is transfered to
                /// another component by e.g. `outgoing-handler.handle`.
                pub fn headers(&self) -> Headers {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-request.headers"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        Fields::from_handle(ret as u32)
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// Construct a default `request-options` value.
                pub fn new() -> Self {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[constructor]request-options"]
                            fn wit_import() -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import() -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import();
                        RequestOptions::from_handle(ret as u32)
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// The timeout for the initial connect to the HTTP Server.
                pub fn connect_timeout(&self) -> Option<Duration> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]request-options.connect-timeout"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the timeout for the initial connect to the HTTP Server. An error
                /// return value indicates that this timeout is not supported.
                pub fn set_connect_timeout(&self, duration: Option<Duration>) -> Result<(), ()> {
                    unsafe {
                        let (result0_0, result0_1) = match duration {
                            Some(e) => (1i32, _rt::as_i64(e)),
                            None => (0i32, 0i64),
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]request-options.set-connect-timeout"]
                            fn wit_import(_: i32, _: i32, _: i64) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i64) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32, result0_0, result0_1);
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// The timeout for receiving the first byte of the Response body.
                pub fn first_byte_timeout(&self) -> Option<Duration> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]request-options.first-byte-timeout"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the timeout for receiving the first byte of the Response body. An
                /// error return value indicates that this timeout is not supported.
                pub fn set_first_byte_timeout(&self, duration: Option<Duration>) -> Result<(), ()> {
                    unsafe {
                        let (result0_0, result0_1) = match duration {
                            Some(e) => (1i32, _rt::as_i64(e)),
                            None => (0i32, 0i64),
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]request-options.set-first-byte-timeout"]
                            fn wit_import(_: i32, _: i32, _: i64) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i64) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32, result0_0, result0_1);
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// The timeout for receiving subsequent chunks of bytes in the Response
                /// body stream.
                pub fn between_bytes_timeout(&self) -> Option<Duration> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]request-options.between-bytes-timeout"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl RequestOptions {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the timeout for receiving subsequent chunks of bytes in the Response
                /// body stream. An error return value indicates that this timeout is not
                /// supported.
                pub fn set_between_bytes_timeout(
                    &self,
                    duration: Option<Duration>,
                ) -> Result<(), ()> {
                    unsafe {
                        let (result0_0, result0_1) = match duration {
                            Some(e) => (1i32, _rt::as_i64(e)),
                            None => (0i32, 0i64),
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]request-options.set-between-bytes-timeout"]
                            fn wit_import(_: i32, _: i32, _: i64) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i64) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32, result0_0, result0_1);
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl ResponseOutparam {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the value of the `response-outparam` to either send a response,
                /// or indicate an error.
                ///
                /// This method consumes the `response-outparam` to ensure that it is
                /// called at most once. If it is never called, the implementation
                /// will respond with an error.
                ///
                /// The user may provide an `error` to `response` to allow the
                /// implementation determine how to respond with an HTTP error response.
                pub fn set(param: ResponseOutparam, response: Result<OutgoingResponse, ErrorCode>) {
                    unsafe {
                        let (
                            result38_0,
                            result38_1,
                            result38_2,
                            result38_3,
                            result38_4,
                            result38_5,
                            result38_6,
                            result38_7,
                        ) = match &response {
                            Ok(e) => (
                                0i32,
                                (e).take_handle() as i32,
                                0i32,
                                ::core::mem::MaybeUninit::<u64>::zeroed(),
                                ::core::ptr::null_mut(),
                                ::core::ptr::null_mut(),
                                0usize,
                                0i32,
                            ),
                            Err(e) => {
                                let (
                                    result37_0,
                                    result37_1,
                                    result37_2,
                                    result37_3,
                                    result37_4,
                                    result37_5,
                                    result37_6,
                                ) = match e {
                                    ErrorCode::DnsTimeout => (
                                        0i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::DnsError(e) => {
                                        let DnsErrorPayload {
                                            rcode: rcode0,
                                            info_code: info_code0,
                                        } = e;
                                        let (result2_0, result2_1, result2_2) = match rcode0 {
                                            Some(e) => {
                                                let vec1 = e;
                                                let ptr1 = vec1.as_ptr().cast::<u8>();
                                                let len1 = vec1.len();
                                                (1i32, ptr1.cast_mut(), len1)
                                            }
                                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                                        };
                                        let (result3_0, result3_1) = match info_code0 {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            1i32,
                                            result2_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result2_1);
                                                t
                                            },
                                            result2_2 as *mut u8,
                                            result3_0 as *mut u8,
                                            result3_1 as usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::DestinationNotFound => (
                                        2i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::DestinationUnavailable => (
                                        3i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::DestinationIpProhibited => (
                                        4i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::DestinationIpUnroutable => (
                                        5i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConnectionRefused => (
                                        6i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConnectionTerminated => (
                                        7i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConnectionTimeout => (
                                        8i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConnectionReadTimeout => (
                                        9i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConnectionWriteTimeout => (
                                        10i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConnectionLimitReached => (
                                        11i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::TlsProtocolError => (
                                        12i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::TlsCertificateError => (
                                        13i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::TlsAlertReceived(e) => {
                                        let TlsAlertReceivedPayload {
                                            alert_id: alert_id4,
                                            alert_message: alert_message4,
                                        } = e;
                                        let (result5_0, result5_1) = match alert_id4 {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        let (result7_0, result7_1, result7_2) = match alert_message4
                                        {
                                            Some(e) => {
                                                let vec6 = e;
                                                let ptr6 = vec6.as_ptr().cast::<u8>();
                                                let len6 = vec6.len();
                                                (1i32, ptr6.cast_mut(), len6)
                                            }
                                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                                        };
                                        (
                                            14i32,
                                            result5_0,
                                            ::core::mem::MaybeUninit::new(
                                                i64::from(result5_1) as u64
                                            ),
                                            result7_0 as *mut u8,
                                            result7_1,
                                            result7_2,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpRequestDenied => (
                                        15i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpRequestLengthRequired => (
                                        16i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpRequestBodySize(e) => {
                                        let (result8_0, result8_1) = match e {
                                            Some(e) => (1i32, _rt::as_i64(e)),
                                            None => (0i32, 0i64),
                                        };
                                        (
                                            17i32,
                                            result8_0,
                                            ::core::mem::MaybeUninit::new(result8_1 as u64),
                                            ::core::ptr::null_mut(),
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpRequestMethodInvalid => (
                                        18i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpRequestUriInvalid => (
                                        19i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpRequestUriTooLong => (
                                        20i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpRequestHeaderSectionSize(e) => {
                                        let (result9_0, result9_1) = match e {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            21i32,
                                            result9_0,
                                            ::core::mem::MaybeUninit::new(
                                                i64::from(result9_1) as u64
                                            ),
                                            ::core::ptr::null_mut(),
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpRequestHeaderSize(e) => {
                                        let (
                                            result14_0,
                                            result14_1,
                                            result14_2,
                                            result14_3,
                                            result14_4,
                                            result14_5,
                                        ) = match e {
                                            Some(e) => {
                                                let FieldSizePayload {
                                                    field_name: field_name10,
                                                    field_size: field_size10,
                                                } = e;
                                                let (result12_0, result12_1, result12_2) =
                                                    match field_name10 {
                                                        Some(e) => {
                                                            let vec11 = e;
                                                            let ptr11 = vec11.as_ptr().cast::<u8>();
                                                            let len11 = vec11.len();
                                                            (1i32, ptr11.cast_mut(), len11)
                                                        }
                                                        None => {
                                                            (0i32, ::core::ptr::null_mut(), 0usize)
                                                        }
                                                    };
                                                let (result13_0, result13_1) = match field_size10 {
                                                    Some(e) => (1i32, _rt::as_i32(e)),
                                                    None => (0i32, 0i32),
                                                };
                                                (
                                                    1i32, result12_0, result12_1, result12_2,
                                                    result13_0, result13_1,
                                                )
                                            }
                                            None => (
                                                0i32,
                                                0i32,
                                                ::core::ptr::null_mut(),
                                                0usize,
                                                0i32,
                                                0i32,
                                            ),
                                        };
                                        (
                                            22i32,
                                            result14_0,
                                            ::core::mem::MaybeUninit::new(
                                                i64::from(result14_1) as u64
                                            ),
                                            result14_2,
                                            result14_3 as *mut u8,
                                            result14_4 as usize,
                                            result14_5,
                                        )
                                    }
                                    ErrorCode::HttpRequestTrailerSectionSize(e) => {
                                        let (result15_0, result15_1) = match e {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            23i32,
                                            result15_0,
                                            ::core::mem::MaybeUninit::new(
                                                i64::from(result15_1) as u64
                                            ),
                                            ::core::ptr::null_mut(),
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpRequestTrailerSize(e) => {
                                        let FieldSizePayload {
                                            field_name: field_name16,
                                            field_size: field_size16,
                                        } = e;
                                        let (result18_0, result18_1, result18_2) =
                                            match field_name16 {
                                                Some(e) => {
                                                    let vec17 = e;
                                                    let ptr17 = vec17.as_ptr().cast::<u8>();
                                                    let len17 = vec17.len();
                                                    (1i32, ptr17.cast_mut(), len17)
                                                }
                                                None => (0i32, ::core::ptr::null_mut(), 0usize),
                                            };
                                        let (result19_0, result19_1) = match field_size16 {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            24i32,
                                            result18_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result18_1);
                                                t
                                            },
                                            result18_2 as *mut u8,
                                            result19_0 as *mut u8,
                                            result19_1 as usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseIncomplete => (
                                        25i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpResponseHeaderSectionSize(e) => {
                                        let (result20_0, result20_1) = match e {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            26i32,
                                            result20_0,
                                            ::core::mem::MaybeUninit::new(
                                                i64::from(result20_1) as u64
                                            ),
                                            ::core::ptr::null_mut(),
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseHeaderSize(e) => {
                                        let FieldSizePayload {
                                            field_name: field_name21,
                                            field_size: field_size21,
                                        } = e;
                                        let (result23_0, result23_1, result23_2) =
                                            match field_name21 {
                                                Some(e) => {
                                                    let vec22 = e;
                                                    let ptr22 = vec22.as_ptr().cast::<u8>();
                                                    let len22 = vec22.len();
                                                    (1i32, ptr22.cast_mut(), len22)
                                                }
                                                None => (0i32, ::core::ptr::null_mut(), 0usize),
                                            };
                                        let (result24_0, result24_1) = match field_size21 {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            27i32,
                                            result23_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result23_1);
                                                t
                                            },
                                            result23_2 as *mut u8,
                                            result24_0 as *mut u8,
                                            result24_1 as usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseBodySize(e) => {
                                        let (result25_0, result25_1) = match e {
                                            Some(e) => (1i32, _rt::as_i64(e)),
                                            None => (0i32, 0i64),
                                        };
                                        (
                                            28i32,
                                            result25_0,
                                            ::core::mem::MaybeUninit::new(result25_1 as u64),
                                            ::core::ptr::null_mut(),
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseTrailerSectionSize(e) => {
                                        let (result26_0, result26_1) = match e {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            29i32,
                                            result26_0,
                                            ::core::mem::MaybeUninit::new(
                                                i64::from(result26_1) as u64
                                            ),
                                            ::core::ptr::null_mut(),
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseTrailerSize(e) => {
                                        let FieldSizePayload {
                                            field_name: field_name27,
                                            field_size: field_size27,
                                        } = e;
                                        let (result29_0, result29_1, result29_2) =
                                            match field_name27 {
                                                Some(e) => {
                                                    let vec28 = e;
                                                    let ptr28 = vec28.as_ptr().cast::<u8>();
                                                    let len28 = vec28.len();
                                                    (1i32, ptr28.cast_mut(), len28)
                                                }
                                                None => (0i32, ::core::ptr::null_mut(), 0usize),
                                            };
                                        let (result30_0, result30_1) = match field_size27 {
                                            Some(e) => (1i32, _rt::as_i32(e)),
                                            None => (0i32, 0i32),
                                        };
                                        (
                                            30i32,
                                            result29_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result29_1);
                                                t
                                            },
                                            result29_2 as *mut u8,
                                            result30_0 as *mut u8,
                                            result30_1 as usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseTransferCoding(e) => {
                                        let (result32_0, result32_1, result32_2) = match e {
                                            Some(e) => {
                                                let vec31 = e;
                                                let ptr31 = vec31.as_ptr().cast::<u8>();
                                                let len31 = vec31.len();
                                                (1i32, ptr31.cast_mut(), len31)
                                            }
                                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                                        };
                                        (
                                            31i32,
                                            result32_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result32_1);
                                                t
                                            },
                                            result32_2 as *mut u8,
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseContentCoding(e) => {
                                        let (result34_0, result34_1, result34_2) = match e {
                                            Some(e) => {
                                                let vec33 = e;
                                                let ptr33 = vec33.as_ptr().cast::<u8>();
                                                let len33 = vec33.len();
                                                (1i32, ptr33.cast_mut(), len33)
                                            }
                                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                                        };
                                        (
                                            32i32,
                                            result34_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result34_1);
                                                t
                                            },
                                            result34_2 as *mut u8,
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                    ErrorCode::HttpResponseTimeout => (
                                        33i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpUpgradeFailed => (
                                        34i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::HttpProtocolError => (
                                        35i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::LoopDetected => (
                                        36i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::ConfigurationError => (
                                        37i32,
                                        0i32,
                                        ::core::mem::MaybeUninit::<u64>::zeroed(),
                                        ::core::ptr::null_mut(),
                                        ::core::ptr::null_mut(),
                                        0usize,
                                        0i32,
                                    ),
                                    ErrorCode::InternalError(e) => {
                                        let (result36_0, result36_1, result36_2) = match e {
                                            Some(e) => {
                                                let vec35 = e;
                                                let ptr35 = vec35.as_ptr().cast::<u8>();
                                                let len35 = vec35.len();
                                                (1i32, ptr35.cast_mut(), len35)
                                            }
                                            None => (0i32, ::core::ptr::null_mut(), 0usize),
                                        };
                                        (
                                            38i32,
                                            result36_0,
                                            {
                                                let mut t =
                                                    ::core::mem::MaybeUninit::<u64>::uninit();
                                                t.as_mut_ptr().cast::<*mut u8>().write(result36_1);
                                                t
                                            },
                                            result36_2 as *mut u8,
                                            ::core::ptr::null_mut(),
                                            0usize,
                                            0i32,
                                        )
                                    }
                                };
                                (
                                    1i32, result37_0, result37_1, result37_2, result37_3,
                                    result37_4, result37_5, result37_6,
                                )
                            }
                        };
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[static]response-outparam.set"]
                            fn wit_import(
                                _: i32,
                                _: i32,
                                _: i32,
                                _: i32,
                                _: ::core::mem::MaybeUninit<u64>,
                                _: *mut u8,
                                _: *mut u8,
                                _: usize,
                                _: i32,
                            );
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(
                            _: i32,
                            _: i32,
                            _: i32,
                            _: i32,
                            _: ::core::mem::MaybeUninit<u64>,
                            _: *mut u8,
                            _: *mut u8,
                            _: usize,
                            _: i32,
                        ) {
                            unreachable!()
                        }
                        wit_import(
                            (&param).take_handle() as i32,
                            result38_0,
                            result38_1,
                            result38_2,
                            result38_3,
                            result38_4,
                            result38_5,
                            result38_6,
                            result38_7,
                        );
                    }
                }
            }
            impl IncomingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the status code from the incoming response.
                pub fn status(&self) -> StatusCode {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-response.status"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        ret as u16
                    }
                }
            }
            impl IncomingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the headers from the incoming response.
                ///
                /// The returned `headers` resource is immutable: `set`, `append`, and
                /// `delete` operations will fail with `header-error.immutable`.
                ///
                /// This headers resource is a child: it must be dropped before the parent
                /// `incoming-response` is dropped.
                pub fn headers(&self) -> Headers {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-response.headers"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        Fields::from_handle(ret as u32)
                    }
                }
            }
            impl IncomingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the incoming body. May be called at most once. Returns error
                /// if called additional times.
                pub fn consume(&self) -> Result<IncomingBody, ()> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-response.consume"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<i32>();
                                    IncomingBody::from_handle(l2 as u32)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl IncomingBody {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the contents of the body, as a stream of bytes.
                ///
                /// Returns success on first call: the stream representing the contents
                /// can be retrieved at most once. Subsequent calls will return error.
                ///
                /// The returned `input-stream` resource is a child: it must be dropped
                /// before the parent `incoming-body` is dropped, or consumed by
                /// `incoming-body.finish`.
                ///
                /// This invariant ensures that the implementation can determine whether
                /// the user is consuming the contents of the body, waiting on the
                /// `future-trailers` to be ready, or neither. This allows for network
                /// backpressure is to be applied when the user is consuming the body,
                /// and for that backpressure to not inhibit delivery of the trailers if
                /// the user does not read the entire body.
                pub fn stream(&self) -> Result<InputStream, ()> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]incoming-body.stream"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<i32>();
                                    super::super::super::wasi::io::streams::InputStream::from_handle(
                                        l2 as u32,
                                    )
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl IncomingBody {
                #[allow(unused_unsafe, clippy::all)]
                /// Takes ownership of `incoming-body`, and returns a `future-trailers`.
                /// This function will trap if the `input-stream` child is still alive.
                pub fn finish(this: IncomingBody) -> FutureTrailers {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[static]incoming-body.finish"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((&this).take_handle() as i32);
                        FutureTrailers::from_handle(ret as u32)
                    }
                }
            }
            impl FutureTrailers {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns a pollable which becomes ready when either the trailers have
                /// been received, or an error has occured. When this pollable is ready,
                /// the `get` method will return `some`.
                pub fn subscribe(&self) -> Pollable {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]future-trailers.subscribe"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        super::super::super::wasi::io::poll::Pollable::from_handle(ret as u32)
                    }
                }
            }
            impl FutureTrailers {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the contents of the trailers, or an error which occured,
                /// once the future is ready.
                ///
                /// The outer `option` represents future readiness. Users can wait on this
                /// `option` to become `some` using the `subscribe` method.
                ///
                /// The outer `result` is used to retrieve the trailers or error at most
                /// once. It will be success on the first call in which the outer option
                /// is `some`, and error on subsequent calls.
                ///
                /// The inner `result` represents that either the HTTP Request or Response
                /// body, as well as any trailers, were received successfully, or that an
                /// error occured receiving them. The optional `trailers` indicates whether
                /// or not trailers were present in the body.
                ///
                /// When some `trailers` are returned by this method, the `trailers`
                /// resource is immutable, and a child. Use of the `set`, `append`, or
                /// `delete` methods will return an error, and the resource must be
                /// dropped before the parent `future-trailers` is dropped.
                pub fn get(&self) -> Option<Result<Result<Option<Trailers>, ErrorCode>, ()>> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 56]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 56]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]future-trailers.get"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(8).cast::<u8>());
                                    match l2 {
                                        0 => {
                                            let e = {
                                                let l3 = i32::from(*ptr0.add(16).cast::<u8>());
                                                match l3 {
                                                    0 => {
                                                        let e = {
                                                            let l4 = i32::from(
                                                                *ptr0.add(24).cast::<u8>(),
                                                            );
                                                            match l4 {
                                                                0 => None,
                                                                1 => {
                                                                    let e = {
                                                                        let l5 = *ptr0
                                                                            .add(28)
                                                                            .cast::<i32>();
                                                                        Fields::from_handle(
                                                                            l5 as u32,
                                                                        )
                                                                    };
                                                                    Some(e)
                                                                }
                                                                _ => {
                                                                    _rt::invalid_enum_discriminant()
                                                                }
                                                            }
                                                        };
                                                        Ok(e)
                                                    }
                                                    1 => {
                                                        let e = {
                                                            let l6 = i32::from(
                                                                *ptr0.add(24).cast::<u8>(),
                                                            );
                                                            let v68 = match l6 {
                                                                0 => ErrorCode::DnsTimeout,
                                                                1 => {
                                                                    let e68 = {
                                                                        let l7 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l11 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        DnsErrorPayload {
                                                                            rcode: match l7 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l8 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l9 = *ptr0.add(40).cast::<usize>();
                                                                                        let len10 = l9;
                                                                                        let bytes10 = _rt::Vec::from_raw_parts(
                                                                                            l8.cast(),
                                                                                            len10,
                                                                                            len10,
                                                                                        );
                                                                                        _rt::string_lift(bytes10)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            info_code: match l11 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l12 = i32::from(*ptr0.add(46).cast::<u16>());
                                                                                        l12 as u16
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::DnsError(e68)
                                                                }
                                                                2 => ErrorCode::DestinationNotFound,
                                                                3 => ErrorCode::DestinationUnavailable,
                                                                4 => ErrorCode::DestinationIpProhibited,
                                                                5 => ErrorCode::DestinationIpUnroutable,
                                                                6 => ErrorCode::ConnectionRefused,
                                                                7 => ErrorCode::ConnectionTerminated,
                                                                8 => ErrorCode::ConnectionTimeout,
                                                                9 => ErrorCode::ConnectionReadTimeout,
                                                                10 => ErrorCode::ConnectionWriteTimeout,
                                                                11 => ErrorCode::ConnectionLimitReached,
                                                                12 => ErrorCode::TlsProtocolError,
                                                                13 => ErrorCode::TlsCertificateError,
                                                                14 => {
                                                                    let e68 = {
                                                                        let l13 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l15 = i32::from(*ptr0.add(36).cast::<u8>());
                                                                        TlsAlertReceivedPayload {
                                                                            alert_id: match l13 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l14 = i32::from(*ptr0.add(33).cast::<u8>());
                                                                                        l14 as u8
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            alert_message: match l15 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l16 = *ptr0.add(40).cast::<*mut u8>();
                                                                                        let l17 = *ptr0.add(44).cast::<usize>();
                                                                                        let len18 = l17;
                                                                                        let bytes18 = _rt::Vec::from_raw_parts(
                                                                                            l16.cast(),
                                                                                            len18,
                                                                                            len18,
                                                                                        );
                                                                                        _rt::string_lift(bytes18)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::TlsAlertReceived(e68)
                                                                }
                                                                15 => ErrorCode::HttpRequestDenied,
                                                                16 => ErrorCode::HttpRequestLengthRequired,
                                                                17 => {
                                                                    let e68 = {
                                                                        let l19 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l19 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l20 = *ptr0.add(40).cast::<i64>();
                                                                                    l20 as u64
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestBodySize(e68)
                                                                }
                                                                18 => ErrorCode::HttpRequestMethodInvalid,
                                                                19 => ErrorCode::HttpRequestUriInvalid,
                                                                20 => ErrorCode::HttpRequestUriTooLong,
                                                                21 => {
                                                                    let e68 = {
                                                                        let l21 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l21 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l22 = *ptr0.add(36).cast::<i32>();
                                                                                    l22 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestHeaderSectionSize(e68)
                                                                }
                                                                22 => {
                                                                    let e68 = {
                                                                        let l23 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l23 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l24 = i32::from(*ptr0.add(36).cast::<u8>());
                                                                                    let l28 = i32::from(*ptr0.add(48).cast::<u8>());
                                                                                    FieldSizePayload {
                                                                                        field_name: match l24 {
                                                                                            0 => None,
                                                                                            1 => {
                                                                                                let e = {
                                                                                                    let l25 = *ptr0.add(40).cast::<*mut u8>();
                                                                                                    let l26 = *ptr0.add(44).cast::<usize>();
                                                                                                    let len27 = l26;
                                                                                                    let bytes27 = _rt::Vec::from_raw_parts(
                                                                                                        l25.cast(),
                                                                                                        len27,
                                                                                                        len27,
                                                                                                    );
                                                                                                    _rt::string_lift(bytes27)
                                                                                                };
                                                                                                Some(e)
                                                                                            }
                                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                                        },
                                                                                        field_size: match l28 {
                                                                                            0 => None,
                                                                                            1 => {
                                                                                                let e = {
                                                                                                    let l29 = *ptr0.add(52).cast::<i32>();
                                                                                                    l29 as u32
                                                                                                };
                                                                                                Some(e)
                                                                                            }
                                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                                        },
                                                                                    }
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestHeaderSize(e68)
                                                                }
                                                                23 => {
                                                                    let e68 = {
                                                                        let l30 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l30 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l31 = *ptr0.add(36).cast::<i32>();
                                                                                    l31 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestTrailerSectionSize(e68)
                                                                }
                                                                24 => {
                                                                    let e68 = {
                                                                        let l32 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l36 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        FieldSizePayload {
                                                                            field_name: match l32 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l33 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l34 = *ptr0.add(40).cast::<usize>();
                                                                                        let len35 = l34;
                                                                                        let bytes35 = _rt::Vec::from_raw_parts(
                                                                                            l33.cast(),
                                                                                            len35,
                                                                                            len35,
                                                                                        );
                                                                                        _rt::string_lift(bytes35)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            field_size: match l36 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l37 = *ptr0.add(48).cast::<i32>();
                                                                                        l37 as u32
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestTrailerSize(e68)
                                                                }
                                                                25 => ErrorCode::HttpResponseIncomplete,
                                                                26 => {
                                                                    let e68 = {
                                                                        let l38 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l38 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l39 = *ptr0.add(36).cast::<i32>();
                                                                                    l39 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseHeaderSectionSize(e68)
                                                                }
                                                                27 => {
                                                                    let e68 = {
                                                                        let l40 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l44 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        FieldSizePayload {
                                                                            field_name: match l40 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l41 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l42 = *ptr0.add(40).cast::<usize>();
                                                                                        let len43 = l42;
                                                                                        let bytes43 = _rt::Vec::from_raw_parts(
                                                                                            l41.cast(),
                                                                                            len43,
                                                                                            len43,
                                                                                        );
                                                                                        _rt::string_lift(bytes43)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            field_size: match l44 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l45 = *ptr0.add(48).cast::<i32>();
                                                                                        l45 as u32
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseHeaderSize(e68)
                                                                }
                                                                28 => {
                                                                    let e68 = {
                                                                        let l46 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l46 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l47 = *ptr0.add(40).cast::<i64>();
                                                                                    l47 as u64
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseBodySize(e68)
                                                                }
                                                                29 => {
                                                                    let e68 = {
                                                                        let l48 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l48 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l49 = *ptr0.add(36).cast::<i32>();
                                                                                    l49 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseTrailerSectionSize(e68)
                                                                }
                                                                30 => {
                                                                    let e68 = {
                                                                        let l50 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l54 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        FieldSizePayload {
                                                                            field_name: match l50 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l51 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l52 = *ptr0.add(40).cast::<usize>();
                                                                                        let len53 = l52;
                                                                                        let bytes53 = _rt::Vec::from_raw_parts(
                                                                                            l51.cast(),
                                                                                            len53,
                                                                                            len53,
                                                                                        );
                                                                                        _rt::string_lift(bytes53)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            field_size: match l54 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l55 = *ptr0.add(48).cast::<i32>();
                                                                                        l55 as u32
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseTrailerSize(e68)
                                                                }
                                                                31 => {
                                                                    let e68 = {
                                                                        let l56 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l56 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l57 = *ptr0.add(36).cast::<*mut u8>();
                                                                                    let l58 = *ptr0.add(40).cast::<usize>();
                                                                                    let len59 = l58;
                                                                                    let bytes59 = _rt::Vec::from_raw_parts(
                                                                                        l57.cast(),
                                                                                        len59,
                                                                                        len59,
                                                                                    );
                                                                                    _rt::string_lift(bytes59)
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseTransferCoding(e68)
                                                                }
                                                                32 => {
                                                                    let e68 = {
                                                                        let l60 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l60 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l61 = *ptr0.add(36).cast::<*mut u8>();
                                                                                    let l62 = *ptr0.add(40).cast::<usize>();
                                                                                    let len63 = l62;
                                                                                    let bytes63 = _rt::Vec::from_raw_parts(
                                                                                        l61.cast(),
                                                                                        len63,
                                                                                        len63,
                                                                                    );
                                                                                    _rt::string_lift(bytes63)
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseContentCoding(e68)
                                                                }
                                                                33 => ErrorCode::HttpResponseTimeout,
                                                                34 => ErrorCode::HttpUpgradeFailed,
                                                                35 => ErrorCode::HttpProtocolError,
                                                                36 => ErrorCode::LoopDetected,
                                                                37 => ErrorCode::ConfigurationError,
                                                                n => {
                                                                    debug_assert_eq!(n, 38, "invalid enum discriminant");
                                                                    let e68 = {
                                                                        let l64 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l64 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l65 = *ptr0.add(36).cast::<*mut u8>();
                                                                                    let l66 = *ptr0.add(40).cast::<usize>();
                                                                                    let len67 = l66;
                                                                                    let bytes67 = _rt::Vec::from_raw_parts(
                                                                                        l65.cast(),
                                                                                        len67,
                                                                                        len67,
                                                                                    );
                                                                                    _rt::string_lift(bytes67)
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::InternalError(e68)
                                                                }
                                                            };
                                                            v68
                                                        };
                                                        Err(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            Ok(e)
                                        }
                                        1 => {
                                            let e = ();
                                            Err(e)
                                        }
                                        _ => _rt::invalid_enum_discriminant(),
                                    }
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Construct an `outgoing-response`, with a default `status-code` of `200`.
                /// If a different `status-code` is needed, it must be set via the
                /// `set-status-code` method.
                ///
                /// * `headers` is the HTTP Headers for the Response.
                pub fn new(headers: Headers) -> Self {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[constructor]outgoing-response"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((&headers).take_handle() as i32);
                        OutgoingResponse::from_handle(ret as u32)
                    }
                }
            }
            impl OutgoingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the HTTP Status Code for the Response.
                pub fn status_code(&self) -> StatusCode {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-response.status-code"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        ret as u16
                    }
                }
            }
            impl OutgoingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Set the HTTP Status Code for the Response. Fails if the status-code
                /// given is not a valid http status code.
                pub fn set_status_code(&self, status_code: StatusCode) -> Result<(), ()> {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-response.set-status-code"]
                            fn wit_import(_: i32, _: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32, _rt::as_i32(status_code));
                        match ret {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Get the headers associated with the Request.
                ///
                /// The returned `headers` resource is immutable: `set`, `append`, and
                /// `delete` operations will fail with `header-error.immutable`.
                ///
                /// This headers resource is a child: it must be dropped before the parent
                /// `outgoing-request` is dropped, or its ownership is transfered to
                /// another component by e.g. `outgoing-handler.handle`.
                pub fn headers(&self) -> Headers {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-response.headers"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        Fields::from_handle(ret as u32)
                    }
                }
            }
            impl OutgoingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the resource corresponding to the outgoing Body for this Response.
                ///
                /// Returns success on the first call: the `outgoing-body` resource for
                /// this `outgoing-response` can be retrieved at most once. Subsequent
                /// calls will return error.
                pub fn body(&self) -> Result<OutgoingBody, ()> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-response.body"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<i32>();
                                    OutgoingBody::from_handle(l2 as u32)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingBody {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns a stream for writing the body contents.
                ///
                /// The returned `output-stream` is a child resource: it must be dropped
                /// before the parent `outgoing-body` resource is dropped (or finished),
                /// otherwise the `outgoing-body` drop or `finish` will trap.
                ///
                /// Returns success on the first call: the `output-stream` resource for
                /// this `outgoing-body` may be retrieved at most once. Subsequent calls
                /// will return error.
                pub fn write(&self) -> Result<OutputStream, ()> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]outgoing-body.write"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<i32>();
                                    super::super::super::wasi::io::streams::OutputStream::from_handle(
                                        l2 as u32,
                                    )
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = ();
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutgoingBody {
                #[allow(unused_unsafe, clippy::all)]
                /// Finalize an outgoing body, optionally providing trailers. This must be
                /// called to signal that the response is complete. If the `outgoing-body`
                /// is dropped without calling `outgoing-body.finalize`, the implementation
                /// should treat the body as corrupted.
                ///
                /// Fails if the body's `outgoing-request` or `outgoing-response` was
                /// constructed with a Content-Length header, and the contents written
                /// to the body (via `write`) does not match the value given in the
                /// Content-Length.
                pub fn finish(
                    this: OutgoingBody,
                    trailers: Option<Trailers>,
                ) -> Result<(), ErrorCode> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 40]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 40]);
                        let (result0_0, result0_1) = match &trailers {
                            Some(e) => (1i32, (e).take_handle() as i32),
                            None => (0i32, 0i32),
                        };
                        let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[static]outgoing-body.finish"]
                            fn wit_import(_: i32, _: i32, _: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((&this).take_handle() as i32, result0_0, result0_1, ptr1);
                        let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                        match l2 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr1.add(8).cast::<u8>());
                                    let v65 = match l3 {
                                        0 => ErrorCode::DnsTimeout,
                                        1 => {
                                            let e65 = {
                                                let l4 = i32::from(*ptr1.add(16).cast::<u8>());
                                                let l8 = i32::from(*ptr1.add(28).cast::<u8>());
                                                DnsErrorPayload {
                                                    rcode: match l4 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l5 =
                                                                    *ptr1.add(20).cast::<*mut u8>();
                                                                let l6 =
                                                                    *ptr1.add(24).cast::<usize>();
                                                                let len7 = l6;
                                                                let bytes7 =
                                                                    _rt::Vec::from_raw_parts(
                                                                        l5.cast(),
                                                                        len7,
                                                                        len7,
                                                                    );
                                                                _rt::string_lift(bytes7)
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                    info_code: match l8 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l9 = i32::from(
                                                                    *ptr1.add(30).cast::<u16>(),
                                                                );
                                                                l9 as u16
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                }
                                            };
                                            ErrorCode::DnsError(e65)
                                        }
                                        2 => ErrorCode::DestinationNotFound,
                                        3 => ErrorCode::DestinationUnavailable,
                                        4 => ErrorCode::DestinationIpProhibited,
                                        5 => ErrorCode::DestinationIpUnroutable,
                                        6 => ErrorCode::ConnectionRefused,
                                        7 => ErrorCode::ConnectionTerminated,
                                        8 => ErrorCode::ConnectionTimeout,
                                        9 => ErrorCode::ConnectionReadTimeout,
                                        10 => ErrorCode::ConnectionWriteTimeout,
                                        11 => ErrorCode::ConnectionLimitReached,
                                        12 => ErrorCode::TlsProtocolError,
                                        13 => ErrorCode::TlsCertificateError,
                                        14 => {
                                            let e65 = {
                                                let l10 = i32::from(*ptr1.add(16).cast::<u8>());
                                                let l12 = i32::from(*ptr1.add(20).cast::<u8>());
                                                TlsAlertReceivedPayload {
                                                    alert_id: match l10 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l11 = i32::from(
                                                                    *ptr1.add(17).cast::<u8>(),
                                                                );
                                                                l11 as u8
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                    alert_message: match l12 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l13 =
                                                                    *ptr1.add(24).cast::<*mut u8>();
                                                                let l14 =
                                                                    *ptr1.add(28).cast::<usize>();
                                                                let len15 = l14;
                                                                let bytes15 =
                                                                    _rt::Vec::from_raw_parts(
                                                                        l13.cast(),
                                                                        len15,
                                                                        len15,
                                                                    );
                                                                _rt::string_lift(bytes15)
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                }
                                            };
                                            ErrorCode::TlsAlertReceived(e65)
                                        }
                                        15 => ErrorCode::HttpRequestDenied,
                                        16 => ErrorCode::HttpRequestLengthRequired,
                                        17 => {
                                            let e65 = {
                                                let l16 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l16 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l17 = *ptr1.add(24).cast::<i64>();
                                                            l17 as u64
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpRequestBodySize(e65)
                                        }
                                        18 => ErrorCode::HttpRequestMethodInvalid,
                                        19 => ErrorCode::HttpRequestUriInvalid,
                                        20 => ErrorCode::HttpRequestUriTooLong,
                                        21 => {
                                            let e65 = {
                                                let l18 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l18 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l19 = *ptr1.add(20).cast::<i32>();
                                                            l19 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpRequestHeaderSectionSize(e65)
                                        }
                                        22 => {
                                            let e65 = {
                                                let l20 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l20 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l21 = i32::from(
                                                                *ptr1.add(20).cast::<u8>(),
                                                            );
                                                            let l25 = i32::from(
                                                                *ptr1.add(32).cast::<u8>(),
                                                            );
                                                            FieldSizePayload {
                                                                field_name: match l21 {
                                                                    0 => None,
                                                                    1 => {
                                                                        let e = {
                                                                            let l22 = *ptr1.add(24).cast::<*mut u8>();
                                                                            let l23 = *ptr1.add(28).cast::<usize>();
                                                                            let len24 = l23;
                                                                            let bytes24 = _rt::Vec::from_raw_parts(
                                                                                l22.cast(),
                                                                                len24,
                                                                                len24,
                                                                            );
                                                                            _rt::string_lift(bytes24)
                                                                        };
                                                                        Some(e)
                                                                    }
                                                                    _ => _rt::invalid_enum_discriminant(),
                                                                },
                                                                field_size: match l25 {
                                                                    0 => None,
                                                                    1 => {
                                                                        let e = {
                                                                            let l26 = *ptr1.add(36).cast::<i32>();
                                                                            l26 as u32
                                                                        };
                                                                        Some(e)
                                                                    }
                                                                    _ => _rt::invalid_enum_discriminant(),
                                                                },
                                                            }
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpRequestHeaderSize(e65)
                                        }
                                        23 => {
                                            let e65 = {
                                                let l27 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l27 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l28 = *ptr1.add(20).cast::<i32>();
                                                            l28 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpRequestTrailerSectionSize(e65)
                                        }
                                        24 => {
                                            let e65 = {
                                                let l29 = i32::from(*ptr1.add(16).cast::<u8>());
                                                let l33 = i32::from(*ptr1.add(28).cast::<u8>());
                                                FieldSizePayload {
                                                    field_name: match l29 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l30 =
                                                                    *ptr1.add(20).cast::<*mut u8>();
                                                                let l31 =
                                                                    *ptr1.add(24).cast::<usize>();
                                                                let len32 = l31;
                                                                let bytes32 =
                                                                    _rt::Vec::from_raw_parts(
                                                                        l30.cast(),
                                                                        len32,
                                                                        len32,
                                                                    );
                                                                _rt::string_lift(bytes32)
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                    field_size: match l33 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l34 =
                                                                    *ptr1.add(32).cast::<i32>();
                                                                l34 as u32
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                }
                                            };
                                            ErrorCode::HttpRequestTrailerSize(e65)
                                        }
                                        25 => ErrorCode::HttpResponseIncomplete,
                                        26 => {
                                            let e65 = {
                                                let l35 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l35 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l36 = *ptr1.add(20).cast::<i32>();
                                                            l36 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpResponseHeaderSectionSize(e65)
                                        }
                                        27 => {
                                            let e65 = {
                                                let l37 = i32::from(*ptr1.add(16).cast::<u8>());
                                                let l41 = i32::from(*ptr1.add(28).cast::<u8>());
                                                FieldSizePayload {
                                                    field_name: match l37 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l38 =
                                                                    *ptr1.add(20).cast::<*mut u8>();
                                                                let l39 =
                                                                    *ptr1.add(24).cast::<usize>();
                                                                let len40 = l39;
                                                                let bytes40 =
                                                                    _rt::Vec::from_raw_parts(
                                                                        l38.cast(),
                                                                        len40,
                                                                        len40,
                                                                    );
                                                                _rt::string_lift(bytes40)
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                    field_size: match l41 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l42 =
                                                                    *ptr1.add(32).cast::<i32>();
                                                                l42 as u32
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                }
                                            };
                                            ErrorCode::HttpResponseHeaderSize(e65)
                                        }
                                        28 => {
                                            let e65 = {
                                                let l43 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l43 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l44 = *ptr1.add(24).cast::<i64>();
                                                            l44 as u64
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpResponseBodySize(e65)
                                        }
                                        29 => {
                                            let e65 = {
                                                let l45 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l45 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l46 = *ptr1.add(20).cast::<i32>();
                                                            l46 as u32
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpResponseTrailerSectionSize(e65)
                                        }
                                        30 => {
                                            let e65 = {
                                                let l47 = i32::from(*ptr1.add(16).cast::<u8>());
                                                let l51 = i32::from(*ptr1.add(28).cast::<u8>());
                                                FieldSizePayload {
                                                    field_name: match l47 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l48 =
                                                                    *ptr1.add(20).cast::<*mut u8>();
                                                                let l49 =
                                                                    *ptr1.add(24).cast::<usize>();
                                                                let len50 = l49;
                                                                let bytes50 =
                                                                    _rt::Vec::from_raw_parts(
                                                                        l48.cast(),
                                                                        len50,
                                                                        len50,
                                                                    );
                                                                _rt::string_lift(bytes50)
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                    field_size: match l51 {
                                                        0 => None,
                                                        1 => {
                                                            let e = {
                                                                let l52 =
                                                                    *ptr1.add(32).cast::<i32>();
                                                                l52 as u32
                                                            };
                                                            Some(e)
                                                        }
                                                        _ => _rt::invalid_enum_discriminant(),
                                                    },
                                                }
                                            };
                                            ErrorCode::HttpResponseTrailerSize(e65)
                                        }
                                        31 => {
                                            let e65 = {
                                                let l53 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l53 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l54 =
                                                                *ptr1.add(20).cast::<*mut u8>();
                                                            let l55 = *ptr1.add(24).cast::<usize>();
                                                            let len56 = l55;
                                                            let bytes56 = _rt::Vec::from_raw_parts(
                                                                l54.cast(),
                                                                len56,
                                                                len56,
                                                            );
                                                            _rt::string_lift(bytes56)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpResponseTransferCoding(e65)
                                        }
                                        32 => {
                                            let e65 = {
                                                let l57 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l57 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l58 =
                                                                *ptr1.add(20).cast::<*mut u8>();
                                                            let l59 = *ptr1.add(24).cast::<usize>();
                                                            let len60 = l59;
                                                            let bytes60 = _rt::Vec::from_raw_parts(
                                                                l58.cast(),
                                                                len60,
                                                                len60,
                                                            );
                                                            _rt::string_lift(bytes60)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::HttpResponseContentCoding(e65)
                                        }
                                        33 => ErrorCode::HttpResponseTimeout,
                                        34 => ErrorCode::HttpUpgradeFailed,
                                        35 => ErrorCode::HttpProtocolError,
                                        36 => ErrorCode::LoopDetected,
                                        37 => ErrorCode::ConfigurationError,
                                        n => {
                                            debug_assert_eq!(n, 38, "invalid enum discriminant");
                                            let e65 = {
                                                let l61 = i32::from(*ptr1.add(16).cast::<u8>());
                                                match l61 {
                                                    0 => None,
                                                    1 => {
                                                        let e = {
                                                            let l62 =
                                                                *ptr1.add(20).cast::<*mut u8>();
                                                            let l63 = *ptr1.add(24).cast::<usize>();
                                                            let len64 = l63;
                                                            let bytes64 = _rt::Vec::from_raw_parts(
                                                                l62.cast(),
                                                                len64,
                                                                len64,
                                                            );
                                                            _rt::string_lift(bytes64)
                                                        };
                                                        Some(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            ErrorCode::InternalError(e65)
                                        }
                                    };
                                    v65
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl FutureIncomingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns a pollable which becomes ready when either the Response has
                /// been received, or an error has occured. When this pollable is ready,
                /// the `get` method will return `some`.
                pub fn subscribe(&self) -> Pollable {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]future-incoming-response.subscribe"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        super::super::super::wasi::io::poll::Pollable::from_handle(ret as u32)
                    }
                }
            }
            impl FutureIncomingResponse {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns the incoming HTTP Response, or an error, once one is ready.
                ///
                /// The outer `option` represents future readiness. Users can wait on this
                /// `option` to become `some` using the `subscribe` method.
                ///
                /// The outer `result` is used to retrieve the response or error at most
                /// once. It will be success on the first call in which the outer option
                /// is `some`, and error on subsequent calls.
                ///
                /// The inner `result` represents that either the incoming HTTP Response
                /// status and headers have recieved successfully, or that an error
                /// occured. Errors may also occur while consuming the response body,
                /// but those will be reported by the `incoming-body` and its
                /// `output-stream` child.
                pub fn get(&self) -> Option<Result<Result<IncomingResponse, ErrorCode>, ()>> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 56]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 56]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:http/types@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]future-incoming-response.get"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => None,
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(8).cast::<u8>());
                                    match l2 {
                                        0 => {
                                            let e = {
                                                let l3 = i32::from(*ptr0.add(16).cast::<u8>());
                                                match l3 {
                                                    0 => {
                                                        let e = {
                                                            let l4 = *ptr0.add(24).cast::<i32>();
                                                            IncomingResponse::from_handle(l4 as u32)
                                                        };
                                                        Ok(e)
                                                    }
                                                    1 => {
                                                        let e = {
                                                            let l5 = i32::from(
                                                                *ptr0.add(24).cast::<u8>(),
                                                            );
                                                            let v67 = match l5 {
                                                                0 => ErrorCode::DnsTimeout,
                                                                1 => {
                                                                    let e67 = {
                                                                        let l6 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l10 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        DnsErrorPayload {
                                                                            rcode: match l6 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l7 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l8 = *ptr0.add(40).cast::<usize>();
                                                                                        let len9 = l8;
                                                                                        let bytes9 = _rt::Vec::from_raw_parts(
                                                                                            l7.cast(),
                                                                                            len9,
                                                                                            len9,
                                                                                        );
                                                                                        _rt::string_lift(bytes9)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            info_code: match l10 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l11 = i32::from(*ptr0.add(46).cast::<u16>());
                                                                                        l11 as u16
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::DnsError(e67)
                                                                }
                                                                2 => ErrorCode::DestinationNotFound,
                                                                3 => ErrorCode::DestinationUnavailable,
                                                                4 => ErrorCode::DestinationIpProhibited,
                                                                5 => ErrorCode::DestinationIpUnroutable,
                                                                6 => ErrorCode::ConnectionRefused,
                                                                7 => ErrorCode::ConnectionTerminated,
                                                                8 => ErrorCode::ConnectionTimeout,
                                                                9 => ErrorCode::ConnectionReadTimeout,
                                                                10 => ErrorCode::ConnectionWriteTimeout,
                                                                11 => ErrorCode::ConnectionLimitReached,
                                                                12 => ErrorCode::TlsProtocolError,
                                                                13 => ErrorCode::TlsCertificateError,
                                                                14 => {
                                                                    let e67 = {
                                                                        let l12 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l14 = i32::from(*ptr0.add(36).cast::<u8>());
                                                                        TlsAlertReceivedPayload {
                                                                            alert_id: match l12 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l13 = i32::from(*ptr0.add(33).cast::<u8>());
                                                                                        l13 as u8
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            alert_message: match l14 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l15 = *ptr0.add(40).cast::<*mut u8>();
                                                                                        let l16 = *ptr0.add(44).cast::<usize>();
                                                                                        let len17 = l16;
                                                                                        let bytes17 = _rt::Vec::from_raw_parts(
                                                                                            l15.cast(),
                                                                                            len17,
                                                                                            len17,
                                                                                        );
                                                                                        _rt::string_lift(bytes17)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::TlsAlertReceived(e67)
                                                                }
                                                                15 => ErrorCode::HttpRequestDenied,
                                                                16 => ErrorCode::HttpRequestLengthRequired,
                                                                17 => {
                                                                    let e67 = {
                                                                        let l18 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l18 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l19 = *ptr0.add(40).cast::<i64>();
                                                                                    l19 as u64
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestBodySize(e67)
                                                                }
                                                                18 => ErrorCode::HttpRequestMethodInvalid,
                                                                19 => ErrorCode::HttpRequestUriInvalid,
                                                                20 => ErrorCode::HttpRequestUriTooLong,
                                                                21 => {
                                                                    let e67 = {
                                                                        let l20 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l20 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l21 = *ptr0.add(36).cast::<i32>();
                                                                                    l21 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestHeaderSectionSize(e67)
                                                                }
                                                                22 => {
                                                                    let e67 = {
                                                                        let l22 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l22 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l23 = i32::from(*ptr0.add(36).cast::<u8>());
                                                                                    let l27 = i32::from(*ptr0.add(48).cast::<u8>());
                                                                                    FieldSizePayload {
                                                                                        field_name: match l23 {
                                                                                            0 => None,
                                                                                            1 => {
                                                                                                let e = {
                                                                                                    let l24 = *ptr0.add(40).cast::<*mut u8>();
                                                                                                    let l25 = *ptr0.add(44).cast::<usize>();
                                                                                                    let len26 = l25;
                                                                                                    let bytes26 = _rt::Vec::from_raw_parts(
                                                                                                        l24.cast(),
                                                                                                        len26,
                                                                                                        len26,
                                                                                                    );
                                                                                                    _rt::string_lift(bytes26)
                                                                                                };
                                                                                                Some(e)
                                                                                            }
                                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                                        },
                                                                                        field_size: match l27 {
                                                                                            0 => None,
                                                                                            1 => {
                                                                                                let e = {
                                                                                                    let l28 = *ptr0.add(52).cast::<i32>();
                                                                                                    l28 as u32
                                                                                                };
                                                                                                Some(e)
                                                                                            }
                                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                                        },
                                                                                    }
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestHeaderSize(e67)
                                                                }
                                                                23 => {
                                                                    let e67 = {
                                                                        let l29 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l29 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l30 = *ptr0.add(36).cast::<i32>();
                                                                                    l30 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestTrailerSectionSize(e67)
                                                                }
                                                                24 => {
                                                                    let e67 = {
                                                                        let l31 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l35 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        FieldSizePayload {
                                                                            field_name: match l31 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l32 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l33 = *ptr0.add(40).cast::<usize>();
                                                                                        let len34 = l33;
                                                                                        let bytes34 = _rt::Vec::from_raw_parts(
                                                                                            l32.cast(),
                                                                                            len34,
                                                                                            len34,
                                                                                        );
                                                                                        _rt::string_lift(bytes34)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            field_size: match l35 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l36 = *ptr0.add(48).cast::<i32>();
                                                                                        l36 as u32
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpRequestTrailerSize(e67)
                                                                }
                                                                25 => ErrorCode::HttpResponseIncomplete,
                                                                26 => {
                                                                    let e67 = {
                                                                        let l37 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l37 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l38 = *ptr0.add(36).cast::<i32>();
                                                                                    l38 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseHeaderSectionSize(e67)
                                                                }
                                                                27 => {
                                                                    let e67 = {
                                                                        let l39 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l43 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        FieldSizePayload {
                                                                            field_name: match l39 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l40 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l41 = *ptr0.add(40).cast::<usize>();
                                                                                        let len42 = l41;
                                                                                        let bytes42 = _rt::Vec::from_raw_parts(
                                                                                            l40.cast(),
                                                                                            len42,
                                                                                            len42,
                                                                                        );
                                                                                        _rt::string_lift(bytes42)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            field_size: match l43 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l44 = *ptr0.add(48).cast::<i32>();
                                                                                        l44 as u32
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseHeaderSize(e67)
                                                                }
                                                                28 => {
                                                                    let e67 = {
                                                                        let l45 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l45 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l46 = *ptr0.add(40).cast::<i64>();
                                                                                    l46 as u64
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseBodySize(e67)
                                                                }
                                                                29 => {
                                                                    let e67 = {
                                                                        let l47 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l47 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l48 = *ptr0.add(36).cast::<i32>();
                                                                                    l48 as u32
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseTrailerSectionSize(e67)
                                                                }
                                                                30 => {
                                                                    let e67 = {
                                                                        let l49 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        let l53 = i32::from(*ptr0.add(44).cast::<u8>());
                                                                        FieldSizePayload {
                                                                            field_name: match l49 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l50 = *ptr0.add(36).cast::<*mut u8>();
                                                                                        let l51 = *ptr0.add(40).cast::<usize>();
                                                                                        let len52 = l51;
                                                                                        let bytes52 = _rt::Vec::from_raw_parts(
                                                                                            l50.cast(),
                                                                                            len52,
                                                                                            len52,
                                                                                        );
                                                                                        _rt::string_lift(bytes52)
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                            field_size: match l53 {
                                                                                0 => None,
                                                                                1 => {
                                                                                    let e = {
                                                                                        let l54 = *ptr0.add(48).cast::<i32>();
                                                                                        l54 as u32
                                                                                    };
                                                                                    Some(e)
                                                                                }
                                                                                _ => _rt::invalid_enum_discriminant(),
                                                                            },
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseTrailerSize(e67)
                                                                }
                                                                31 => {
                                                                    let e67 = {
                                                                        let l55 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l55 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l56 = *ptr0.add(36).cast::<*mut u8>();
                                                                                    let l57 = *ptr0.add(40).cast::<usize>();
                                                                                    let len58 = l57;
                                                                                    let bytes58 = _rt::Vec::from_raw_parts(
                                                                                        l56.cast(),
                                                                                        len58,
                                                                                        len58,
                                                                                    );
                                                                                    _rt::string_lift(bytes58)
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseTransferCoding(e67)
                                                                }
                                                                32 => {
                                                                    let e67 = {
                                                                        let l59 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l59 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l60 = *ptr0.add(36).cast::<*mut u8>();
                                                                                    let l61 = *ptr0.add(40).cast::<usize>();
                                                                                    let len62 = l61;
                                                                                    let bytes62 = _rt::Vec::from_raw_parts(
                                                                                        l60.cast(),
                                                                                        len62,
                                                                                        len62,
                                                                                    );
                                                                                    _rt::string_lift(bytes62)
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::HttpResponseContentCoding(e67)
                                                                }
                                                                33 => ErrorCode::HttpResponseTimeout,
                                                                34 => ErrorCode::HttpUpgradeFailed,
                                                                35 => ErrorCode::HttpProtocolError,
                                                                36 => ErrorCode::LoopDetected,
                                                                37 => ErrorCode::ConfigurationError,
                                                                n => {
                                                                    debug_assert_eq!(n, 38, "invalid enum discriminant");
                                                                    let e67 = {
                                                                        let l63 = i32::from(*ptr0.add(32).cast::<u8>());
                                                                        match l63 {
                                                                            0 => None,
                                                                            1 => {
                                                                                let e = {
                                                                                    let l64 = *ptr0.add(36).cast::<*mut u8>();
                                                                                    let l65 = *ptr0.add(40).cast::<usize>();
                                                                                    let len66 = l65;
                                                                                    let bytes66 = _rt::Vec::from_raw_parts(
                                                                                        l64.cast(),
                                                                                        len66,
                                                                                        len66,
                                                                                    );
                                                                                    _rt::string_lift(bytes66)
                                                                                };
                                                                                Some(e)
                                                                            }
                                                                            _ => _rt::invalid_enum_discriminant(),
                                                                        }
                                                                    };
                                                                    ErrorCode::InternalError(e67)
                                                                }
                                                            };
                                                            v67
                                                        };
                                                        Err(e)
                                                    }
                                                    _ => _rt::invalid_enum_discriminant(),
                                                }
                                            };
                                            Ok(e)
                                        }
                                        1 => {
                                            let e = ();
                                            Err(e)
                                        }
                                        _ => _rt::invalid_enum_discriminant(),
                                    }
                                };
                                Some(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod io {
        #[allow(dead_code, clippy::all)]
        pub mod poll {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            /// `pollable` represents a single I/O event which may be ready, or not.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct Pollable {
                handle: _rt::Resource<Pollable>,
            }
            impl Pollable {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for Pollable {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:io/poll@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]pollable"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            impl Pollable {
                #[allow(unused_unsafe, clippy::all)]
                /// Return the readiness of a pollable. This function never blocks.
                ///
                /// Returns `true` when the pollable is ready, and `false` otherwise.
                pub fn ready(&self) -> bool {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/poll@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]pollable.ready"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        _rt::bool_lift(ret as u8)
                    }
                }
            }
            impl Pollable {
                #[allow(unused_unsafe, clippy::all)]
                /// `block` returns immediately if the pollable is ready, and otherwise
                /// blocks until ready.
                ///
                /// This function is equivalent to calling `poll.poll` on a list
                /// containing only this pollable.
                pub fn block(&self) {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/poll@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]pollable.block"]
                            fn wit_import(_: i32);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32);
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Poll for completion on a set of pollables.
            ///
            /// This function takes a list of pollables, which identify I/O sources of
            /// interest, and waits until one or more of the events is ready for I/O.
            ///
            /// The result `list<u32>` contains one or more indices of handles in the
            /// argument list that is ready for I/O.
            ///
            /// If the list contains more elements than can be indexed with a `u32`
            /// value, this function traps.
            ///
            /// A timeout can be implemented by adding a pollable from the
            /// wasi-clocks API to the list.
            ///
            /// This function does not return a `result`; polling in itself does not
            /// do any I/O so it doesn't fail. If any of the I/O sources identified by
            /// the pollables has an error, it is indicated by marking the source as
            /// being reaedy for I/O.
            pub fn poll(in_: &[&Pollable]) -> _rt::Vec<u32> {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = in_;
                    let len0 = vec0.len();
                    let layout0 = _rt::alloc::Layout::from_size_align_unchecked(vec0.len() * 4, 4);
                    let result0 = if layout0.size() != 0 {
                        let ptr = _rt::alloc::alloc(layout0).cast::<u8>();
                        if ptr.is_null() {
                            _rt::alloc::handle_alloc_error(layout0);
                        }
                        ptr
                    } else {
                        {
                            ::core::ptr::null_mut()
                        }
                    };
                    for (i, e) in vec0.into_iter().enumerate() {
                        let base = result0.add(i * 4);
                        {
                            *base.add(0).cast::<i32>() = (e).handle() as i32;
                        }
                    }
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:io/poll@0.2.0")]
                    extern "C" {
                        #[link_name = "poll"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(result0, len0, ptr1);
                    let l2 = *ptr1.add(0).cast::<*mut u8>();
                    let l3 = *ptr1.add(4).cast::<usize>();
                    let len4 = l3;
                    if layout0.size() != 0 {
                        _rt::alloc::dealloc(result0.cast(), layout0);
                    }
                    _rt::Vec::from_raw_parts(l2.cast(), len4, len4)
                }
            }
        }
        #[allow(dead_code, clippy::all)]
        pub mod error {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            /// A resource which represents some error information.
            ///
            /// The only method provided by this resource is `to-debug-string`,
            /// which provides some human-readable information about the error.
            ///
            /// In the `wasi:io` package, this resource is returned through the
            /// `wasi:io/streams/stream-error` type.
            ///
            /// To provide more specific error information, other interfaces may
            /// provide functions to further "downcast" this error into more specific
            /// error information. For example, `error`s returned in streams derived
            /// from filesystem types to be described using the filesystem's own
            /// error-code type, using the function
            /// `wasi:filesystem/types/filesystem-error-code`, which takes a parameter
            /// `borrow<error>` and returns
            /// `option<wasi:filesystem/types/error-code>`.
            ///
            /// The set of functions which can "downcast" an `error` into a more
            /// concrete type is open.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct Error {
                handle: _rt::Resource<Error>,
            }
            impl Error {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for Error {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:io/error@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]error"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            impl Error {
                #[allow(unused_unsafe, clippy::all)]
                /// Returns a string that is suitable to assist humans in debugging
                /// this error.
                ///
                /// WARNING: The returned string should not be consumed mechanically!
                /// It may change across platforms, hosts, or other implementation
                /// details. Parsing this string is a major platform-compatibility
                /// hazard.
                pub fn to_debug_string(&self) -> _rt::String {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/error@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]error.to-debug-string"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = *ptr0.add(0).cast::<*mut u8>();
                        let l2 = *ptr0.add(4).cast::<usize>();
                        let len3 = l2;
                        let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                        _rt::string_lift(bytes3)
                    }
                }
            }
        }
        #[allow(dead_code, clippy::all)]
        pub mod streams {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type Error = super::super::super::wasi::io::error::Error;
            pub type Pollable = super::super::super::wasi::io::poll::Pollable;
            /// An error for input-stream and output-stream operations.
            pub enum StreamError {
                /// The last operation (a write or flush) failed before completion.
                ///
                /// More information is available in the `error` payload.
                LastOperationFailed(Error),
                /// The stream is closed: no more input will be accepted by the
                /// stream. A closed output-stream will return this error on all
                /// future operations.
                Closed,
            }
            impl ::core::fmt::Debug for StreamError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        StreamError::LastOperationFailed(e) => f
                            .debug_tuple("StreamError::LastOperationFailed")
                            .field(e)
                            .finish(),
                        StreamError::Closed => f.debug_tuple("StreamError::Closed").finish(),
                    }
                }
            }
            impl ::core::fmt::Display for StreamError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    write!(f, "{:?}", self)
                }
            }
            impl std::error::Error for StreamError {}
            /// An input bytestream.
            ///
            /// `input-stream`s are *non-blocking* to the extent practical on underlying
            /// platforms. I/O operations always return promptly; if fewer bytes are
            /// promptly available than requested, they return the number of bytes promptly
            /// available, which could even be zero. To wait for data to be available,
            /// use the `subscribe` function to obtain a `pollable` which can be polled
            /// for using `wasi:io/poll`.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct InputStream {
                handle: _rt::Resource<InputStream>,
            }
            impl InputStream {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for InputStream {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]input-stream"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            /// An output bytestream.
            ///
            /// `output-stream`s are *non-blocking* to the extent practical on
            /// underlying platforms. Except where specified otherwise, I/O operations also
            /// always return promptly, after the number of bytes that can be written
            /// promptly, which could even be zero. To wait for the stream to be ready to
            /// accept data, the `subscribe` function to obtain a `pollable` which can be
            /// polled for using `wasi:io/poll`.
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct OutputStream {
                handle: _rt::Resource<OutputStream>,
            }
            impl OutputStream {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for OutputStream {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[resource-drop]output-stream"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            impl InputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Perform a non-blocking read from the stream.
                ///
                /// When the source of a `read` is binary data, the bytes from the source
                /// are returned verbatim. When the source of a `read` is known to the
                /// implementation to be text, bytes containing the UTF-8 encoding of the
                /// text are returned.
                ///
                /// This function returns a list of bytes containing the read data,
                /// when successful. The returned list will contain up to `len` bytes;
                /// it may return fewer than requested, but not more. The list is
                /// empty when no bytes are available for reading at this time. The
                /// pollable given by `subscribe` will be ready when more bytes are
                /// available.
                ///
                /// This function fails with a `stream-error` when the operation
                /// encounters an error, giving `last-operation-failed`, or when the
                /// stream is closed, giving `closed`.
                ///
                /// When the caller gives a `len` of 0, it represents a request to
                /// read 0 bytes. If the stream is still open, this call should
                /// succeed and return an empty list, or otherwise fail with `closed`.
                ///
                /// The `len` parameter is a `u64`, which could represent a list of u8 which
                /// is not possible to allocate in wasm32, or not desirable to allocate as
                /// as a return value by the callee. The callee may return a list of bytes
                /// less than `len` in size while more bytes are available for reading.
                pub fn read(&self, len: u64) -> Result<_rt::Vec<u8>, StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]input-stream.read"]
                            fn wit_import(_: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, _rt::as_i64(&len), ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    _rt::Vec::from_raw_parts(l2.cast(), len4, len4)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l5 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v7 = match l5 {
                                        0 => {
                                            let e7 = {
                                                let l6 = *ptr0.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l6 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e7)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v7
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl InputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Read bytes from a stream, after blocking until at least one byte can
                /// be read. Except for blocking, behavior is identical to `read`.
                pub fn blocking_read(&self, len: u64) -> Result<_rt::Vec<u8>, StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]input-stream.blocking-read"]
                            fn wit_import(_: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, _rt::as_i64(&len), ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(4).cast::<*mut u8>();
                                    let l3 = *ptr0.add(8).cast::<usize>();
                                    let len4 = l3;
                                    _rt::Vec::from_raw_parts(l2.cast(), len4, len4)
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l5 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v7 = match l5 {
                                        0 => {
                                            let e7 = {
                                                let l6 = *ptr0.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l6 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e7)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v7
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl InputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Skip bytes from a stream. Returns number of bytes skipped.
                ///
                /// Behaves identical to `read`, except instead of returning a list
                /// of bytes, returns the number of bytes consumed from the stream.
                pub fn skip(&self, len: u64) -> Result<u64, StreamError> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]input-stream.skip"]
                            fn wit_import(_: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, _rt::as_i64(&len), ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr0.add(8).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr0.add(12).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl InputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Skip bytes from a stream, after blocking until at least one byte
                /// can be skipped. Except for blocking behavior, identical to `skip`.
                pub fn blocking_skip(&self, len: u64) -> Result<u64, StreamError> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]input-stream.blocking-skip"]
                            fn wit_import(_: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, _rt::as_i64(&len), ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr0.add(8).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr0.add(12).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl InputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Create a `pollable` which will resolve once either the specified stream
                /// has bytes available to read or the other end of the stream has been
                /// closed.
                /// The created `pollable` is a child resource of the `input-stream`.
                /// Implementations may trap if the `input-stream` is dropped before
                /// all derived `pollable`s created with this function are dropped.
                pub fn subscribe(&self) -> Pollable {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]input-stream.subscribe"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        super::super::super::wasi::io::poll::Pollable::from_handle(ret as u32)
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Check readiness for writing. This function never blocks.
                ///
                /// Returns the number of bytes permitted for the next call to `write`,
                /// or an error. Calling `write` with more bytes than this function has
                /// permitted will trap.
                ///
                /// When this function returns 0 bytes, the `subscribe` pollable will
                /// become ready when this function will report at least 1 byte, or an
                /// error.
                pub fn check_write(&self) -> Result<u64, StreamError> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.check-write"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr0.add(8).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr0.add(12).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Perform a write. This function never blocks.
                ///
                /// When the destination of a `write` is binary data, the bytes from
                /// `contents` are written verbatim. When the destination of a `write` is
                /// known to the implementation to be text, the bytes of `contents` are
                /// transcoded from UTF-8 into the encoding of the destination and then
                /// written.
                ///
                /// Precondition: check-write gave permit of Ok(n) and contents has a
                /// length of less than or equal to n. Otherwise, this function will trap.
                ///
                /// returns Err(closed) without writing if the stream has closed since
                /// the last call to check-write provided a permit.
                pub fn write(&self, contents: &[u8]) -> Result<(), StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let vec0 = contents;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.write"]
                            fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0.cast_mut(), len0, ptr1);
                        let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                        match l2 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr1.add(4).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr1.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Perform a write of up to 4096 bytes, and then flush the stream. Block
                /// until all of these operations are complete, or an error occurs.
                ///
                /// This is a convenience wrapper around the use of `check-write`,
                /// `subscribe`, `write`, and `flush`, and is implemented with the
                /// following pseudo-code:
                ///
                /// ```text
                /// let pollable = this.subscribe();
                /// while !contents.is_empty() {
                /// // Wait for the stream to become writable
                /// pollable.block();
                /// let Ok(n) = this.check-write(); // eliding error handling
                /// let len = min(n, contents.len());
                /// let (chunk, rest) = contents.split_at(len);
                /// this.write(chunk  );            // eliding error handling
                /// contents = rest;
                /// }
                /// this.flush();
                /// // Wait for completion of `flush`
                /// pollable.block();
                /// // Check for any errors that arose during `flush`
                /// let _ = this.check-write();         // eliding error handling
                /// ```
                pub fn blocking_write_and_flush(&self, contents: &[u8]) -> Result<(), StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let vec0 = contents;
                        let ptr0 = vec0.as_ptr().cast::<u8>();
                        let len0 = vec0.len();
                        let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.blocking-write-and-flush"]
                            fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0.cast_mut(), len0, ptr1);
                        let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                        match l2 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr1.add(4).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr1.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Request to flush buffered output. This function never blocks.
                ///
                /// This tells the output-stream that the caller intends any buffered
                /// output to be flushed. the output which is expected to be flushed
                /// is all that has been passed to `write` prior to this call.
                ///
                /// Upon calling this function, the `output-stream` will not accept any
                /// writes (`check-write` will return `ok(0)`) until the flush has
                /// completed. The `subscribe` pollable will become ready when the
                /// flush has completed and the stream can accept more writes.
                pub fn flush(&self) -> Result<(), StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.flush"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v4 = match l2 {
                                        0 => {
                                            let e4 = {
                                                let l3 = *ptr0.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l3 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e4)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v4
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Request to flush buffered output, and block until flush completes
                /// and stream is ready for writing again.
                pub fn blocking_flush(&self) -> Result<(), StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.blocking-flush"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v4 = match l2 {
                                        0 => {
                                            let e4 = {
                                                let l3 = *ptr0.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l3 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e4)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v4
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Create a `pollable` which will resolve once the output-stream
                /// is ready for more writing, or an error has occured. When this
                /// pollable is ready, `check-write` will return `ok(n)` with n>0, or an
                /// error.
                ///
                /// If the stream is closed, this pollable is always ready immediately.
                ///
                /// The created `pollable` is a child resource of the `output-stream`.
                /// Implementations may trap if the `output-stream` is dropped before
                /// all derived `pollable`s created with this function are dropped.
                pub fn subscribe(&self) -> Pollable {
                    unsafe {
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.subscribe"]
                            fn wit_import(_: i32) -> i32;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32) -> i32 {
                            unreachable!()
                        }
                        let ret = wit_import((self).handle() as i32);
                        super::super::super::wasi::io::poll::Pollable::from_handle(ret as u32)
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Write zeroes to a stream.
                ///
                /// This should be used precisely like `write` with the exact same
                /// preconditions (must use check-write first), but instead of
                /// passing a list of bytes, you simply pass the number of zero-bytes
                /// that should be written.
                pub fn write_zeroes(&self, len: u64) -> Result<(), StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.write-zeroes"]
                            fn wit_import(_: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, _rt::as_i64(&len), ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v4 = match l2 {
                                        0 => {
                                            let e4 = {
                                                let l3 = *ptr0.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l3 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e4)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v4
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Perform a write of up to 4096 zeroes, and then flush the stream.
                /// Block until all of these operations are complete, or an error
                /// occurs.
                ///
                /// This is a convenience wrapper around the use of `check-write`,
                /// `subscribe`, `write-zeroes`, and `flush`, and is implemented with
                /// the following pseudo-code:
                ///
                /// ```text
                /// let pollable = this.subscribe();
                /// while num_zeroes != 0 {
                /// // Wait for the stream to become writable
                /// pollable.block();
                /// let Ok(n) = this.check-write(); // eliding error handling
                /// let len = min(n, num_zeroes);
                /// this.write-zeroes(len);         // eliding error handling
                /// num_zeroes -= len;
                /// }
                /// this.flush();
                /// // Wait for completion of `flush`
                /// pollable.block();
                /// // Check for any errors that arose during `flush`
                /// let _ = this.check-write();         // eliding error handling
                /// ```
                pub fn blocking_write_zeroes_and_flush(&self, len: u64) -> Result<(), StreamError> {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.blocking-write-zeroes-and-flush"]
                            fn wit_import(_: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, _rt::as_i64(&len), ptr0);
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = ();
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l2 = i32::from(*ptr0.add(4).cast::<u8>());
                                    let v4 = match l2 {
                                        0 => {
                                            let e4 = {
                                                let l3 = *ptr0.add(8).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l3 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e4)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v4
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Read from one stream and write to another.
                ///
                /// The behavior of splice is equivelant to:
                /// 1. calling `check-write` on the `output-stream`
                /// 2. calling `read` on the `input-stream` with the smaller of the
                /// `check-write` permitted length and the `len` provided to `splice`
                /// 3. calling `write` on the `output-stream` with that read data.
                ///
                /// Any error reported by the call to `check-write`, `read`, or
                /// `write` ends the splice and reports that error.
                ///
                /// This function returns the number of bytes transferred; it may be less
                /// than `len`.
                pub fn splice(&self, src: &InputStream, len: u64) -> Result<u64, StreamError> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.splice"]
                            fn wit_import(_: i32, _: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import(
                            (self).handle() as i32,
                            (src).handle() as i32,
                            _rt::as_i64(&len),
                            ptr0,
                        );
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr0.add(8).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr0.add(12).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
            impl OutputStream {
                #[allow(unused_unsafe, clippy::all)]
                /// Read from one stream and write to another, with blocking.
                ///
                /// This is similar to `splice`, except that it blocks until the
                /// `output-stream` is ready for writing, and the `input-stream`
                /// is ready for reading, before performing the `splice`.
                pub fn blocking_splice(
                    &self,
                    src: &InputStream,
                    len: u64,
                ) -> Result<u64, StreamError> {
                    unsafe {
                        #[repr(align(8))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "wasi:io/streams@0.2.0")]
                        extern "C" {
                            #[link_name = "[method]output-stream.blocking-splice"]
                            fn wit_import(_: i32, _: i32, _: i64, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: i32, _: i64, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import(
                            (self).handle() as i32,
                            (src).handle() as i32,
                            _rt::as_i64(&len),
                            ptr0,
                        );
                        let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                        match l1 {
                            0 => {
                                let e = {
                                    let l2 = *ptr0.add(8).cast::<i64>();
                                    l2 as u64
                                };
                                Ok(e)
                            }
                            1 => {
                                let e = {
                                    let l3 = i32::from(*ptr0.add(8).cast::<u8>());
                                    let v5 = match l3 {
                                        0 => {
                                            let e5 = {
                                                let l4 = *ptr0.add(12).cast::<i32>();
                                                super::super::super::wasi::io::error::Error::from_handle(
                                                    l4 as u32,
                                                )
                                            };
                                            StreamError::LastOperationFailed(e5)
                                        }
                                        n => {
                                            debug_assert_eq!(n, 1, "invalid enum discriminant");
                                            StreamError::Closed
                                        }
                                    };
                                    v5
                                };
                                Err(e)
                            }
                            _ => _rt::invalid_enum_discriminant(),
                        }
                    }
                }
            }
        }
    }
}
#[allow(dead_code)]
pub mod exports {
    #[allow(dead_code)]
    pub mod wasi {
        #[allow(dead_code)]
        pub mod http {
            #[allow(dead_code, clippy::all)]
            pub mod incoming_handler {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                pub type IncomingRequest =
                    super::super::super::super::wasi::http::types::IncomingRequest;
                pub type ResponseOutparam =
                    super::super::super::super::wasi::http::types::ResponseOutparam;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_handle_cabi<T: Guest>(arg0: i32, arg1: i32) {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    T::handle(
                        super::super::super::super::wasi::http::types::IncomingRequest::from_handle(
                            arg0 as u32,
                        ),
                        super::super::super::super::wasi::http::types::ResponseOutparam::from_handle(
                            arg1 as u32,
                        ),
                    );
                }
                pub trait Guest {
                    /// This function is invoked with an incoming HTTP Request, and a resource
                    /// `response-outparam` which provides the capability to reply with an HTTP
                    /// Response. The response is sent by calling the `response-outparam.set`
                    /// method, which allows execution to continue after the response has been
                    /// sent. This enables both streaming to the response body, and performing other
                    /// work.
                    ///
                    /// The implementor of this function must write a response to the
                    /// `response-outparam` before returning, or else the caller will respond
                    /// with an error on its behalf.
                    fn handle(request: IncomingRequest, response_out: ResponseOutparam);
                }
                #[doc(hidden)]
                macro_rules! __export_wasi_http_incoming_handler_0_2_0_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name =
                        "wasi:http/incoming-handler@0.2.0#handle"] unsafe extern "C" fn
                        export_handle(arg0 : i32, arg1 : i32,) { $($path_to_types)*::
                        _export_handle_cabi::<$ty > (arg0, arg1) } };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_wasi_http_incoming_handler_0_2_0_cabi;
            }
        }
    }
}
mod _rt {
    pub use alloc_crate::string::String;
    pub fn as_i64<T: AsI64>(t: T) -> i64 {
        t.as_i64()
    }
    pub trait AsI64 {
        fn as_i64(self) -> i64;
    }
    impl<'a, T: Copy + AsI64> AsI64 for &'a T {
        fn as_i64(self) -> i64 {
            (*self).as_i64()
        }
    }
    impl AsI64 for i64 {
        #[inline]
        fn as_i64(self) -> i64 {
            self as i64
        }
    }
    impl AsI64 for u64 {
        #[inline]
        fn as_i64(self) -> i64 {
            self as i64
        }
    }
    pub use alloc_crate::vec::Vec;
    pub unsafe fn string_lift(bytes: Vec<u8>) -> String {
        if cfg!(debug_assertions) {
            String::from_utf8(bytes).unwrap()
        } else {
            String::from_utf8_unchecked(bytes)
        }
    }
    pub fn as_i32<T: AsI32>(t: T) -> i32 {
        t.as_i32()
    }
    pub trait AsI32 {
        fn as_i32(self) -> i32;
    }
    impl<'a, T: Copy + AsI32> AsI32 for &'a T {
        fn as_i32(self) -> i32 {
            (*self).as_i32()
        }
    }
    impl AsI32 for i32 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for u32 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for i16 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for u16 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for i8 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for u8 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for char {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for usize {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    use core::fmt;
    use core::marker;
    use core::sync::atomic::{AtomicU32, Ordering::Relaxed};
    /// A type which represents a component model resource, either imported or
    /// exported into this component.
    ///
    /// This is a low-level wrapper which handles the lifetime of the resource
    /// (namely this has a destructor). The `T` provided defines the component model
    /// intrinsics that this wrapper uses.
    ///
    /// One of the chief purposes of this type is to provide `Deref` implementations
    /// to access the underlying data when it is owned.
    ///
    /// This type is primarily used in generated code for exported and imported
    /// resources.
    #[repr(transparent)]
    pub struct Resource<T: WasmResource> {
        handle: AtomicU32,
        _marker: marker::PhantomData<T>,
    }
    /// A trait which all wasm resources implement, namely providing the ability to
    /// drop a resource.
    ///
    /// This generally is implemented by generated code, not user-facing code.
    #[allow(clippy::missing_safety_doc)]
    pub unsafe trait WasmResource {
        /// Invokes the `[resource-drop]...` intrinsic.
        unsafe fn drop(handle: u32);
    }
    impl<T: WasmResource> Resource<T> {
        #[doc(hidden)]
        pub unsafe fn from_handle(handle: u32) -> Self {
            debug_assert!(handle != u32::MAX);
            Self {
                handle: AtomicU32::new(handle),
                _marker: marker::PhantomData,
            }
        }
        /// Takes ownership of the handle owned by `resource`.
        ///
        /// Note that this ideally would be `into_handle` taking `Resource<T>` by
        /// ownership. The code generator does not enable that in all situations,
        /// unfortunately, so this is provided instead.
        ///
        /// Also note that `take_handle` is in theory only ever called on values
        /// owned by a generated function. For example a generated function might
        /// take `Resource<T>` as an argument but then call `take_handle` on a
        /// reference to that argument. In that sense the dynamic nature of
        /// `take_handle` should only be exposed internally to generated code, not
        /// to user code.
        #[doc(hidden)]
        pub fn take_handle(resource: &Resource<T>) -> u32 {
            resource.handle.swap(u32::MAX, Relaxed)
        }
        #[doc(hidden)]
        pub fn handle(resource: &Resource<T>) -> u32 {
            resource.handle.load(Relaxed)
        }
    }
    impl<T: WasmResource> fmt::Debug for Resource<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Resource")
                .field("handle", &self.handle)
                .finish()
        }
    }
    impl<T: WasmResource> Drop for Resource<T> {
        fn drop(&mut self) {
            unsafe {
                match self.handle.load(Relaxed) {
                    u32::MAX => {}
                    other => T::drop(other),
                }
            }
        }
    }
    pub unsafe fn bool_lift(val: u8) -> bool {
        if cfg!(debug_assertions) {
            match val {
                0 => false,
                1 => true,
                _ => panic!("invalid bool discriminant"),
            }
        } else {
            val != 0
        }
    }
    pub use alloc_crate::alloc;
    pub unsafe fn invalid_enum_discriminant<T>() -> T {
        if cfg!(debug_assertions) {
            panic!("invalid enum discriminant")
        } else {
            core::hint::unreachable_unchecked()
        }
    }
    pub unsafe fn cabi_dealloc(ptr: *mut u8, size: usize, align: usize) {
        if size == 0 {
            return;
        }
        let layout = alloc::Layout::from_size_align_unchecked(size, align);
        alloc::dealloc(ptr, layout);
    }
    #[cfg(target_arch = "wasm32")]
    pub fn run_ctors_once() {
        wit_bindgen_rt::run_ctors_once();
    }
    extern crate alloc as alloc_crate;
}
/// Generates `#[no_mangle]` functions to export the specified type as the
/// root implementation of all generated traits.
///
/// For more information see the documentation of `wit_bindgen::generate!`.
///
/// ```rust
/// # macro_rules! export{ ($($t:tt)*) => (); }
/// # trait Guest {}
/// struct MyType;
///
/// impl Guest for MyType {
///     // ...
/// }
///
/// export!(MyType);
/// ```
#[allow(unused_macros)]
#[doc(hidden)]
macro_rules! __export_any_impl {
    ($ty:ident) => {
        self::export!($ty with_types_in self);
    };
    ($ty:ident with_types_in $($path_to_types_root:tt)*) => {
        $($path_to_types_root)*::
        exports::wasi::http::incoming_handler::__export_wasi_http_incoming_handler_0_2_0_cabi!($ty
        with_types_in $($path_to_types_root)*:: exports::wasi::http::incoming_handler);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 6889] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xef4\x01A\x02\x01A\x1f\
\x01B\x06\x01w\x04\0\x08duration\x03\0\0\x01r\x02\x07secondsw\x0bnanosecondsy\x04\
\0\x08datetime\x03\0\x02\x01q\x03\x03now\0\0\x02at\x01\x03\0\x02in\x01\x01\0\x04\
\0\x0bschedule-at\x03\0\x04\x03\x01\x12obelisk:types/time\x05\0\x01B\x04\x01s\x04\
\0\x0bjoin-set-id\x03\0\0\x01s\x04\0\x0cexecution-id\x03\0\x02\x03\x01\x17obelis\
k:types/execution\x05\x01\x02\x03\0\0\x08duration\x02\x03\0\x01\x0bjoin-set-id\x01\
B\x08\x02\x03\x02\x01\x02\x04\0\x08duration\x03\0\0\x02\x03\x02\x01\x03\x04\0\x0b\
join-set-id\x03\0\x02\x01@\x01\x05nanos\x01\x01\0\x04\0\x05sleep\x01\x04\x01@\0\0\
\x03\x04\0\x0cnew-join-set\x01\x05\x03\x01\x20obelisk:workflow/host-activities\x05\
\x04\x01B\x04\x01@\x02\x01n}\x0aiterationsy\0w\x04\0\x05fibow\x01\0\x04\0\x05fib\
oa\x01\0\x04\0\x10fiboa-concurrent\x01\0\x03\x01\x1etesting:fibo-workflow/workfl\
ow\x05\x05\x01B\x04\x01@\x03\x0bjoin-set-ids\x01n}\x0aiterationsy\0s\x04\0\x0cfi\
boa-submit\x01\0\x01@\x01\x0bjoin-set-ids\0w\x04\0\x10fiboa-await-next\x01\x01\x03\
\x01*testing:fibo-workflow-obelisk-ext/workflow\x05\x06\x01B\x0a\x04\0\x08pollab\
le\x03\x01\x01h\0\x01@\x01\x04self\x01\0\x7f\x04\0\x16[method]pollable.ready\x01\
\x02\x01@\x01\x04self\x01\x01\0\x04\0\x16[method]pollable.block\x01\x03\x01p\x01\
\x01py\x01@\x01\x02in\x04\0\x05\x04\0\x04poll\x01\x06\x03\x01\x12wasi:io/poll@0.\
2.0\x05\x07\x02\x03\0\x05\x08pollable\x01B\x0f\x02\x03\x02\x01\x08\x04\0\x08poll\
able\x03\0\0\x01w\x04\0\x07instant\x03\0\x02\x01w\x04\0\x08duration\x03\0\x04\x01\
@\0\0\x03\x04\0\x03now\x01\x06\x01@\0\0\x05\x04\0\x0aresolution\x01\x07\x01i\x01\
\x01@\x01\x04when\x03\0\x08\x04\0\x11subscribe-instant\x01\x09\x01@\x01\x04when\x05\
\0\x08\x04\0\x12subscribe-duration\x01\x0a\x03\x01!wasi:clocks/monotonic-clock@0\
.2.0\x05\x09\x01B\x04\x04\0\x05error\x03\x01\x01h\0\x01@\x01\x04self\x01\0s\x04\0\
\x1d[method]error.to-debug-string\x01\x02\x03\x01\x13wasi:io/error@0.2.0\x05\x0a\
\x02\x03\0\x07\x05error\x01B(\x02\x03\x02\x01\x0b\x04\0\x05error\x03\0\0\x02\x03\
\x02\x01\x08\x04\0\x08pollable\x03\0\x02\x01i\x01\x01q\x02\x15last-operation-fai\
led\x01\x04\0\x06closed\0\0\x04\0\x0cstream-error\x03\0\x05\x04\0\x0cinput-strea\
m\x03\x01\x04\0\x0doutput-stream\x03\x01\x01h\x07\x01p}\x01j\x01\x0a\x01\x06\x01\
@\x02\x04self\x09\x03lenw\0\x0b\x04\0\x19[method]input-stream.read\x01\x0c\x04\0\
\"[method]input-stream.blocking-read\x01\x0c\x01j\x01w\x01\x06\x01@\x02\x04self\x09\
\x03lenw\0\x0d\x04\0\x19[method]input-stream.skip\x01\x0e\x04\0\"[method]input-s\
tream.blocking-skip\x01\x0e\x01i\x03\x01@\x01\x04self\x09\0\x0f\x04\0\x1e[method\
]input-stream.subscribe\x01\x10\x01h\x08\x01@\x01\x04self\x11\0\x0d\x04\0![metho\
d]output-stream.check-write\x01\x12\x01j\0\x01\x06\x01@\x02\x04self\x11\x08conte\
nts\x0a\0\x13\x04\0\x1b[method]output-stream.write\x01\x14\x04\0.[method]output-\
stream.blocking-write-and-flush\x01\x14\x01@\x01\x04self\x11\0\x13\x04\0\x1b[met\
hod]output-stream.flush\x01\x15\x04\0$[method]output-stream.blocking-flush\x01\x15\
\x01@\x01\x04self\x11\0\x0f\x04\0\x1f[method]output-stream.subscribe\x01\x16\x01\
@\x02\x04self\x11\x03lenw\0\x13\x04\0\"[method]output-stream.write-zeroes\x01\x17\
\x04\05[method]output-stream.blocking-write-zeroes-and-flush\x01\x17\x01@\x03\x04\
self\x11\x03src\x09\x03lenw\0\x0d\x04\0\x1c[method]output-stream.splice\x01\x18\x04\
\0%[method]output-stream.blocking-splice\x01\x18\x03\x01\x15wasi:io/streams@0.2.\
0\x05\x0c\x02\x03\0\x06\x08duration\x02\x03\0\x08\x0cinput-stream\x02\x03\0\x08\x0d\
output-stream\x01B\xc0\x01\x02\x03\x02\x01\x0d\x04\0\x08duration\x03\0\0\x02\x03\
\x02\x01\x0e\x04\0\x0cinput-stream\x03\0\x02\x02\x03\x02\x01\x0f\x04\0\x0doutput\
-stream\x03\0\x04\x02\x03\x02\x01\x0b\x04\0\x08io-error\x03\0\x06\x02\x03\x02\x01\
\x08\x04\0\x08pollable\x03\0\x08\x01q\x0a\x03get\0\0\x04head\0\0\x04post\0\0\x03\
put\0\0\x06delete\0\0\x07connect\0\0\x07options\0\0\x05trace\0\0\x05patch\0\0\x05\
other\x01s\0\x04\0\x06method\x03\0\x0a\x01q\x03\x04HTTP\0\0\x05HTTPS\0\0\x05othe\
r\x01s\0\x04\0\x06scheme\x03\0\x0c\x01ks\x01k{\x01r\x02\x05rcode\x0e\x09info-cod\
e\x0f\x04\0\x11DNS-error-payload\x03\0\x10\x01k}\x01r\x02\x08alert-id\x12\x0dale\
rt-message\x0e\x04\0\x1aTLS-alert-received-payload\x03\0\x13\x01ky\x01r\x02\x0af\
ield-name\x0e\x0afield-size\x15\x04\0\x12field-size-payload\x03\0\x16\x01kw\x01k\
\x17\x01q'\x0bDNS-timeout\0\0\x09DNS-error\x01\x11\0\x15destination-not-found\0\0\
\x17destination-unavailable\0\0\x19destination-IP-prohibited\0\0\x19destination-\
IP-unroutable\0\0\x12connection-refused\0\0\x15connection-terminated\0\0\x12conn\
ection-timeout\0\0\x17connection-read-timeout\0\0\x18connection-write-timeout\0\0\
\x18connection-limit-reached\0\0\x12TLS-protocol-error\0\0\x15TLS-certificate-er\
ror\0\0\x12TLS-alert-received\x01\x14\0\x13HTTP-request-denied\0\0\x1cHTTP-reque\
st-length-required\0\0\x16HTTP-request-body-size\x01\x18\0\x1bHTTP-request-metho\
d-invalid\0\0\x18HTTP-request-URI-invalid\0\0\x19HTTP-request-URI-too-long\0\0\x20\
HTTP-request-header-section-size\x01\x15\0\x18HTTP-request-header-size\x01\x19\0\
!HTTP-request-trailer-section-size\x01\x15\0\x19HTTP-request-trailer-size\x01\x17\
\0\x18HTTP-response-incomplete\0\0!HTTP-response-header-section-size\x01\x15\0\x19\
HTTP-response-header-size\x01\x17\0\x17HTTP-response-body-size\x01\x18\0\"HTTP-r\
esponse-trailer-section-size\x01\x15\0\x1aHTTP-response-trailer-size\x01\x17\0\x1d\
HTTP-response-transfer-coding\x01\x0e\0\x1cHTTP-response-content-coding\x01\x0e\0\
\x15HTTP-response-timeout\0\0\x13HTTP-upgrade-failed\0\0\x13HTTP-protocol-error\0\
\0\x0dloop-detected\0\0\x13configuration-error\0\0\x0einternal-error\x01\x0e\0\x04\
\0\x0aerror-code\x03\0\x1a\x01q\x03\x0einvalid-syntax\0\0\x09forbidden\0\0\x09im\
mutable\0\0\x04\0\x0cheader-error\x03\0\x1c\x01s\x04\0\x09field-key\x03\0\x1e\x01\
p}\x04\0\x0bfield-value\x03\0\x20\x04\0\x06fields\x03\x01\x04\0\x07headers\x03\0\
\"\x04\0\x08trailers\x03\0\"\x04\0\x10incoming-request\x03\x01\x04\0\x10outgoing\
-request\x03\x01\x04\0\x0frequest-options\x03\x01\x04\0\x11response-outparam\x03\
\x01\x01{\x04\0\x0bstatus-code\x03\0)\x04\0\x11incoming-response\x03\x01\x04\0\x0d\
incoming-body\x03\x01\x04\0\x0ffuture-trailers\x03\x01\x04\0\x11outgoing-respons\
e\x03\x01\x04\0\x0doutgoing-body\x03\x01\x04\0\x18future-incoming-response\x03\x01\
\x01i\"\x01@\0\01\x04\0\x13[constructor]fields\x012\x01o\x02\x1f!\x01p3\x01j\x01\
1\x01\x1d\x01@\x01\x07entries4\05\x04\0\x18[static]fields.from-list\x016\x01h\"\x01\
p!\x01@\x02\x04self7\x04name\x1f\08\x04\0\x12[method]fields.get\x019\x01@\x02\x04\
self7\x04name\x1f\0\x7f\x04\0\x12[method]fields.has\x01:\x01j\0\x01\x1d\x01@\x03\
\x04self7\x04name\x1f\x05value8\0;\x04\0\x12[method]fields.set\x01<\x01@\x02\x04\
self7\x04name\x1f\0;\x04\0\x15[method]fields.delete\x01=\x01@\x03\x04self7\x04na\
me\x1f\x05value!\0;\x04\0\x15[method]fields.append\x01>\x01@\x01\x04self7\04\x04\
\0\x16[method]fields.entries\x01?\x01@\x01\x04self7\01\x04\0\x14[method]fields.c\
lone\x01@\x01h%\x01@\x01\x04self\xc1\0\0\x0b\x04\0\x1f[method]incoming-request.m\
ethod\x01B\x01@\x01\x04self\xc1\0\0\x0e\x04\0([method]incoming-request.path-with\
-query\x01C\x01k\x0d\x01@\x01\x04self\xc1\0\0\xc4\0\x04\0\x1f[method]incoming-re\
quest.scheme\x01E\x04\0\"[method]incoming-request.authority\x01C\x01i#\x01@\x01\x04\
self\xc1\0\0\xc6\0\x04\0\x20[method]incoming-request.headers\x01G\x01i,\x01j\x01\
\xc8\0\0\x01@\x01\x04self\xc1\0\0\xc9\0\x04\0\x20[method]incoming-request.consum\
e\x01J\x01i&\x01@\x01\x07headers\xc6\0\0\xcb\0\x04\0\x1d[constructor]outgoing-re\
quest\x01L\x01h&\x01i/\x01j\x01\xce\0\0\x01@\x01\x04self\xcd\0\0\xcf\0\x04\0\x1d\
[method]outgoing-request.body\x01P\x01@\x01\x04self\xcd\0\0\x0b\x04\0\x1f[method\
]outgoing-request.method\x01Q\x01j\0\0\x01@\x02\x04self\xcd\0\x06method\x0b\0\xd2\
\0\x04\0#[method]outgoing-request.set-method\x01S\x01@\x01\x04self\xcd\0\0\x0e\x04\
\0([method]outgoing-request.path-with-query\x01T\x01@\x02\x04self\xcd\0\x0fpath-\
with-query\x0e\0\xd2\0\x04\0,[method]outgoing-request.set-path-with-query\x01U\x01\
@\x01\x04self\xcd\0\0\xc4\0\x04\0\x1f[method]outgoing-request.scheme\x01V\x01@\x02\
\x04self\xcd\0\x06scheme\xc4\0\0\xd2\0\x04\0#[method]outgoing-request.set-scheme\
\x01W\x04\0\"[method]outgoing-request.authority\x01T\x01@\x02\x04self\xcd\0\x09a\
uthority\x0e\0\xd2\0\x04\0&[method]outgoing-request.set-authority\x01X\x01@\x01\x04\
self\xcd\0\0\xc6\0\x04\0\x20[method]outgoing-request.headers\x01Y\x01i'\x01@\0\0\
\xda\0\x04\0\x1c[constructor]request-options\x01[\x01h'\x01k\x01\x01@\x01\x04sel\
f\xdc\0\0\xdd\0\x04\0'[method]request-options.connect-timeout\x01^\x01@\x02\x04s\
elf\xdc\0\x08duration\xdd\0\0\xd2\0\x04\0+[method]request-options.set-connect-ti\
meout\x01_\x04\0*[method]request-options.first-byte-timeout\x01^\x04\0.[method]r\
equest-options.set-first-byte-timeout\x01_\x04\0-[method]request-options.between\
-bytes-timeout\x01^\x04\01[method]request-options.set-between-bytes-timeout\x01_\
\x01i(\x01i.\x01j\x01\xe1\0\x01\x1b\x01@\x02\x05param\xe0\0\x08response\xe2\0\x01\
\0\x04\0\x1d[static]response-outparam.set\x01c\x01h+\x01@\x01\x04self\xe4\0\0*\x04\
\0\x20[method]incoming-response.status\x01e\x01@\x01\x04self\xe4\0\0\xc6\0\x04\0\
![method]incoming-response.headers\x01f\x01@\x01\x04self\xe4\0\0\xc9\0\x04\0![me\
thod]incoming-response.consume\x01g\x01h,\x01i\x03\x01j\x01\xe9\0\0\x01@\x01\x04\
self\xe8\0\0\xea\0\x04\0\x1c[method]incoming-body.stream\x01k\x01i-\x01@\x01\x04\
this\xc8\0\0\xec\0\x04\0\x1c[static]incoming-body.finish\x01m\x01h-\x01i\x09\x01\
@\x01\x04self\xee\0\0\xef\0\x04\0![method]future-trailers.subscribe\x01p\x01i$\x01\
k\xf1\0\x01j\x01\xf2\0\x01\x1b\x01j\x01\xf3\0\0\x01k\xf4\0\x01@\x01\x04self\xee\0\
\0\xf5\0\x04\0\x1b[method]future-trailers.get\x01v\x01@\x01\x07headers\xc6\0\0\xe1\
\0\x04\0\x1e[constructor]outgoing-response\x01w\x01h.\x01@\x01\x04self\xf8\0\0*\x04\
\0%[method]outgoing-response.status-code\x01y\x01@\x02\x04self\xf8\0\x0bstatus-c\
ode*\0\xd2\0\x04\0)[method]outgoing-response.set-status-code\x01z\x01@\x01\x04se\
lf\xf8\0\0\xc6\0\x04\0![method]outgoing-response.headers\x01{\x01@\x01\x04self\xf8\
\0\0\xcf\0\x04\0\x1e[method]outgoing-response.body\x01|\x01h/\x01i\x05\x01j\x01\xfe\
\0\0\x01@\x01\x04self\xfd\0\0\xff\0\x04\0\x1b[method]outgoing-body.write\x01\x80\
\x01\x01j\0\x01\x1b\x01@\x02\x04this\xce\0\x08trailers\xf2\0\0\x81\x01\x04\0\x1c\
[static]outgoing-body.finish\x01\x82\x01\x01h0\x01@\x01\x04self\x83\x01\0\xef\0\x04\
\0*[method]future-incoming-response.subscribe\x01\x84\x01\x01i+\x01j\x01\x85\x01\
\x01\x1b\x01j\x01\x86\x01\0\x01k\x87\x01\x01@\x01\x04self\x83\x01\0\x88\x01\x04\0\
$[method]future-incoming-response.get\x01\x89\x01\x01h\x07\x01k\x1b\x01@\x01\x03\
err\x8a\x01\0\x8b\x01\x04\0\x0fhttp-error-code\x01\x8c\x01\x03\x01\x15wasi:http/\
types@0.2.0\x05\x10\x02\x03\0\x09\x10incoming-request\x02\x03\0\x09\x11response-\
outparam\x01B\x08\x02\x03\x02\x01\x11\x04\0\x10incoming-request\x03\0\0\x02\x03\x02\
\x01\x12\x04\0\x11response-outparam\x03\0\x02\x01i\x01\x01i\x03\x01@\x02\x07requ\
est\x04\x0cresponse-out\x05\x01\0\x04\0\x06handle\x01\x06\x04\x01\x20wasi:http/i\
ncoming-handler@0.2.0\x05\x13\x04\x01\x0bany:any/any\x04\0\x0b\x09\x01\0\x03any\x03\
\0\0\0G\x09producers\x01\x0cprocessed-by\x02\x0dwit-component\x070.215.0\x10wit-\
bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
