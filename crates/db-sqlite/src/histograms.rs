use hashbrown::HashMap;
use hdrhistogram::{Counter, Histogram};
use std::time::Duration;
use tracing::debug;

pub(crate) struct Histograms {
    metrics_threshold: Option<Duration>,
    // measure how long a transaction (closure) waits until it is processed.
    sending_latency: Histogram<u32>,
    funcions: HashMap<&'static str, Histogram<u32>>,
}
impl Histograms {
    pub(crate) fn new(metrics_threshold: Option<Duration>) -> Histograms {
        let mut send_hist = Histogram::<u32>::new_with_bounds(1, 1_000_000, 3).unwrap();
        send_hist.auto(true);

        Histograms {
            metrics_threshold,
            sending_latency: send_hist,
            funcions: HashMap::new(),
        }
    }

    pub(crate) fn record(
        &mut self,
        sent_latency: Duration,
        func_name: &'static str,
        func_duration: Duration,
    ) {
        if self.metrics_threshold.is_some() {
            if let Ok(value) = u32::try_from(sent_latency.as_micros()) {
                _ = self
                    .sending_latency
                    .record(u64::from(value))
                    .inspect_err(|err| debug!("metric not recorded - {err:?}"));
            }

            if let Ok(value) = u32::try_from(func_duration.as_micros()) {
                let _ = self
                    .funcions
                    .entry(func_name)
                    .or_insert_with(|| {
                        let mut h = Histogram::<u32>::new_with_bounds(1, 1_000_000, 3).unwrap();
                        h.auto(true);
                        h
                    })
                    .record(u64::from(value))
                    .inspect_err(|err| debug!("metric not recorded - {err:?}"));
            }
        }
    }

    pub(crate) fn print_if_elapsed(&mut self, metrics_instant: &mut std::time::Instant) {
        let print_histogram = |name, histogram: &Histogram<u32>, trailing_coma| {
            print!(
                "\"{name}\": {mean}, \"{name}_len\": {len}, \"{name}_meanlen\": {meanlen} {coma}",
                mean = histogram.mean(),
                len = histogram.len(),
                meanlen = histogram.mean() * histogram.len().as_f64(),
                coma = if trailing_coma { "," } else { "" }
            );
        };

        if let Some(metrics_threshold) = self.metrics_threshold
            && metrics_instant.elapsed() > metrics_threshold
        {
            print!("{{");
            print_histogram("send_latency", &self.sending_latency, false);
            self.sending_latency.clear();
            self.funcions.iter_mut().for_each(|(name, h)| {
                print_histogram(*name, h, true);
                h.clear();
            });
            println!("}}");
            *metrics_instant = std::time::Instant::now();
        }
    }
}
