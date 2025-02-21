use std::fmt;
use std::str::FromStr;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::{
    format::{self, FormatEvent, FormatFields},
    FmtContext, FormattedFields,
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
            "{} {: <5} ",
            timestamp.format(":%M:%S%.3f"),
            metadata.level(),
        )?;

        // Write fields on the event
        ctx.field_format().format_fields(writer.by_ref(), event)?;

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

        std::panic::set_hook(Box::new(utils::tracing_panic_hook));

        let fmt_layer = tracing_subscriber::fmt::layer().event_format(MessageFirstFormatter);

        let builder = tracing_subscriber::registry()
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
