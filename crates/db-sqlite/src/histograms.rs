use hashbrown::HashMap;
use hdrhistogram::Histogram;
use std::time::Duration;
use tracing::debug;

pub(crate) struct Histograms {
    metrics_threshold: Option<Duration>,
    // measure how long a transaction (closure) waits until it is processed.
    sending_micros: Histogram<u32>,
    all_fns_micros: Histogram<u32>,
    commit_micros: Histogram<u32>,
    funcions: HashMap<&'static str, Histogram<u32>>,
    last_print: std::time::Instant,
}
impl Histograms {
    pub(crate) fn new(metrics_threshold: Option<Duration>) -> Histograms {
        Histograms {
            metrics_threshold,
            sending_micros: Histogram::<u32>::new(3).unwrap(),
            all_fns_micros: Histogram::<u32>::new(3).unwrap(),
            commit_micros: Histogram::<u32>::new(3).unwrap(),
            funcions: HashMap::new(),
            last_print: std::time::Instant::now(),
        }
    }

    pub(crate) fn record_command(
        &mut self,
        sent_latency: Duration,
        func_name: &'static str,
        func_duration: Duration,
    ) {
        if self.metrics_threshold.is_some() {
            if let Ok(value) = u32::try_from(sent_latency.as_micros()) {
                _ = self
                    .sending_micros
                    .record(u64::from(value))
                    .inspect_err(|err| debug!("metric for sending_micros not recorded - {err:?}"));
            } else {
                debug!("metric for sending_micros would overflow");
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
                    .inspect_err(|err| debug!("metric for {func_name} not recorded - {err:?}"));
            } else {
                debug!("metric for {func_name} would overflow");
            }
        }
    }

    pub(crate) fn record_all_fns(&mut self, all_fn_latency: Duration) {
        if self.metrics_threshold.is_some() {
            if let Ok(value) = u32::try_from(all_fn_latency.as_micros()) {
                _ = self
                    .all_fns_micros
                    .record(u64::from(value))
                    .inspect_err(|err| debug!("metric for all_fns_micros not recorded - {err:?}"));
            } else {
                debug!("metric for all_fns_micros would overflow");
            }
        }
    }

    pub(crate) fn record_commit(&mut self, commit_latency: Duration) {
        if self.metrics_threshold.is_some() {
            if let Ok(value) = u32::try_from(commit_latency.as_micros()) {
                _ = self
                    .commit_micros
                    .record(u64::from(value))
                    .inspect_err(|err| debug!("metric for commit_micros not recorded - {err:?}"));
            } else {
                debug!("metric for commit_micros would overflow");
            }
        }
    }

    pub(crate) fn print_if_elapsed(&mut self) {
        let print_histogram = |name, histogram: &Histogram<u32>| {
            print!(
                "\"{name}_len\": {len}, \"{name}\": {mean},",
                len = histogram.len(),
                mean = histogram.mean(),
            );
        };

        if let Some(metrics_threshold) = self.metrics_threshold
            && self.last_print.elapsed() > metrics_threshold
        {
            print!("{{");
            print_histogram("send_μs", &self.sending_micros);
            print!(r#""all_fns_μs": {},"#, self.all_fns_micros.mean());
            print_histogram("commit_μs", &self.commit_micros);

            // self.funcions.iter_mut().for_each(|(name, h)| {
            //     print_histogram(*name, h, true);
            // });

            println!("}}");
            self.last_print = std::time::Instant::now();
            self.clear();
        }
    }

    fn clear(&mut self) {
        self.sending_micros.clear();
        self.all_fns_micros.clear();
        self.commit_micros.clear();
        self.funcions.clear();
    }
}
