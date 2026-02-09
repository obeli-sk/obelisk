use concepts::component_id::{CONTENT_DIGEST_DUMMY, InputContentDigest};
use concepts::storage::{
    ExecutionEvent, ExecutionLog, ExecutionRequest, JoinSetResponseEventOuter, Locked,
    PendingState, Version,
};
use concepts::{
    ExecutionFailureKind, ExecutionId, FinishedExecutionError, SupportedFunctionReturnValue,
};
use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng as _};
use serde::Serialize;
use std::str::FromStr;
use std::{env::VarError, fmt};
use tracing::{Event, Subscriber};
use tracing_error::ErrorLayer;
use tracing_subscriber::fmt::{
    FmtContext, FormattedFields,
    format::{self, FormatEvent, FormatFields},
};
use tracing_subscriber::registry::LookupSpan;

pub mod arbitrary;
pub mod sim_clock;

static INIT: std::sync::Once = std::sync::Once::new();

struct MessageFirstFormatter;

impl<S, N> FormatEvent<S, N> for MessageFirstFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Format values from the event's's metadata:
        let metadata = event.metadata();
        let timestamp = chrono::Utc::now();
        write!(
            &mut writer,
            "{ts} {level: <5} ",
            ts = timestamp.format(":%M:%S%.3f"),
            level = metadata.level(),
        )?;

        // file:line
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        if let Some(file) = metadata.file()
            && let Some(line) = metadata.line()
        {
            write!(&mut writer, " {file}:{line} ")?;
        }

        // target
        write!(&mut writer, " Target({}) ", metadata.target(),)?;

        // Format all the spans in the event's span context.
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                write!(writer, "{}", span.name())?;

                // `FormattedFields` is a formatted representation of the span's
                // fields, which is stored in its extensions by the `fmt` layer's
                // `new_span` method. The fields will have been formatted
                // by the same field formatter that's provided to the event
                // formatter in the `FmtContext`.
                let ext = span.extensions();
                let fields = &ext
                    .get::<FormattedFields<N>>()
                    .expect("will never be `None`");

                // Skip formatting the fields if the span had no fields.
                if !fields.is_empty() {
                    write!(writer, "{{{fields}}}")?;
                }
                write!(writer, ": ")?;
            }
        }
        writeln!(writer)
    }
}

pub fn set_up() {
    INIT.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        std::panic::set_hook(Box::new(utils::panic_hook::tracing_panic_hook));

        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("default tls provider must be installed");

        let fmt_layer = tracing_subscriber::fmt::layer().event_format(MessageFirstFormatter);

        let builder = tracing_subscriber::registry()
            .with(ErrorLayer::default())
            .with(fmt_layer)
            .with(tracing_subscriber::EnvFilter::from_default_env());

        builder.init();
    });
}

pub fn env_or_default<T: FromStr>(env_var: &str, default: T) -> T {
    std::env::var(env_var)
        .ok()
        .and_then(|val| str::parse(&val).ok())
        .unwrap_or(default)
}

#[must_use]
pub fn get_seed() -> Box<dyn Iterator<Item = u64>> {
    match std::env::var("TEST_SEED") {
        Ok(seed) => Box::new(std::iter::once(seed.parse().unwrap())),
        Err(VarError::NotPresent) => {
            let count = env_or_default("TEST_SEED_NUM", 1);
            let mut vec = Vec::with_capacity(count);
            for _ in 0..count {
                let seed = StdRng::from_rng(&mut rand::rng()).random();
                vec.push(seed);
            }
            Box::new(vec.into_iter().inspect(|&seed| {
                println!("\n\n\nTEST_SEED={seed}");
            }))
        }
        _ => unreachable!(),
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ExecutionLogSanitized {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub responses: Vec<JoinSetResponseEventOuter>,
    pub next_version: Version,
    pub pending_state: PendingState,
    pub component_digest: InputContentDigest,
}
impl From<ExecutionLog> for ExecutionLogSanitized {
    fn from(mut value: ExecutionLog) -> Self {
        for event in value.events.as_mut_slice() {
            match &mut event.event {
                ExecutionRequest::Created { component_id, .. }
                | ExecutionRequest::Locked(Locked { component_id, .. }) => {
                    component_id.input_digest = InputContentDigest(CONTENT_DIGEST_DUMMY);
                }
                ExecutionRequest::Finished {
                    result:
                        SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
                            kind: ExecutionFailureKind::Uncategorized,
                            reason: _,
                            detail,
                        }),
                    ..
                } => {
                    // `execution_error`'s detail (backtrace) removed
                    *detail = None;
                }
                _ => {}
            }
        }

        ExecutionLogSanitized {
            execution_id: value.execution_id,
            events: value.events,
            responses: value.responses.into_iter().map(|resp| resp.event).collect(), // `created_at`, `cursor` removed
            next_version: value.next_version,
            pending_state: value.pending_state,
            component_digest: InputContentDigest(CONTENT_DIGEST_DUMMY),
        }
    }
}
