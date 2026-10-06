//! Runs logical transactions (LTXes) over stateless batches, without an interactive session.
//!
//! The LTX closures are synchronous, so an unknown read cannot wait for the server. Instead the
//! worker re-runs every closure once per round and this module answers from recorded outcomes:
//! - An unknown read stops its LTX for the round; later LTXes keep running, so all reads known to
//!   be needed travel in one batch and the round count follows read depth, not the LTX count.
//! - An `execute` runs speculatively with an assumed row count, verified before COMMIT.
//! - A dry-run batch replays the log on the server and rolls back, returning fresh outcomes.
//! - Once every outcome holds for the final statement order, one batch commits all the writes.
//!
//! A read holds only for the writes that preceded it, tracked as a hash of those writes. Commits
//! bump a single row epoch, which is read in every batch and guarded on commit, so any commit by
//! another node invalidates all known outcomes. That makes the result serializable across nodes
//! at the cost of serializing them globally. The old outcomes stay as speculation: the next dry
//! run sends the whole log at once, keeping the window between validation and COMMIT short.
use crate::sql_connection::{
    BatchStep, Condition, PipelineError, QueryResult, SqlParams, SqlPipeline, StepOutcome,
};
use hashbrown::HashMap;
use rusqlite::types::Value;
use sha2::{Digest, Sha256};
use std::{cell::RefCell, sync::Arc, time::Duration};
use tracing::{debug, warn};

type Result<T> = rusqlite::Result<T>;
type Prefix = [u8; 32];

/// Must be applied once to every database before it is used through [`Replay`].
pub const EPOCH_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS t_write_epoch (id INTEGER PRIMARY KEY CHECK (id = 0), epoch INTEGER NOT NULL);\
    INSERT OR IGNORE INTO t_write_epoch (id, epoch) VALUES (0, 0);";
const EPOCH_READ: &str = "SELECT epoch FROM t_write_epoch";
const EPOCH_BUMP: &str = "UPDATE t_write_epoch SET epoch = epoch + 1";
const MAX_ROUNDS: u32 = 64;
/// Each LTX reading what the previous one wrote takes a round per LTX, splitting bounds the wait.
const MAX_ROUNDS_BEFORE_SPLIT: u32 = 16;
/// Every conflict means another node committed, so these do not count as rounds.
const MAX_EPOCH_CONFLICTS: u32 = 256;
const MAX_CONSECUTIVE_TRANSPORT_FAILURES: u32 = 3;
/// Larger batches are split when more than one LTX contributes to them.
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_HINTS: usize = 4096;

#[derive(Debug, thiserror::Error)]
#[error("statement outcome is not known yet")]
struct OutcomePending;

fn pending() -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(OutcomePending))
}

fn replay_error(reason: String) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(PipelineError::Failed(reason)))
}

#[derive(Clone)]
enum Outcome {
    Done(Arc<QueryResult>),
    Error { code: String, message: String },
}
impl Outcome {
    fn to_result(&self) -> Result<Arc<QueryResult>> {
        match self {
            Self::Done(result) => Ok(result.clone()),
            Self::Error { code, message } => Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(if code.starts_with("SQLITE_CONSTRAINT") {
                    rusqlite::ffi::SQLITE_CONSTRAINT
                } else {
                    rusqlite::ffi::SQLITE_ERROR
                }),
                Some(message.clone()),
            )),
        }
    }
}

struct Entry {
    sql: String,
    params: SqlParams,
    is_write: bool,
    want_rows: bool,
    outcome: Option<Outcome>,
    /// Hash of the writes preceding this statement in the current round.
    prefix: Prefix,
    /// The prefix under which the server reported `outcome`; `None` for an assumed outcome.
    observed_under: Option<Prefix>,
}
impl Entry {
    fn observed(&self) -> bool {
        self.observed_under == Some(self.prefix)
    }
    /// Commit can verify a plain row count on the server, everything else needs a dry run.
    fn needs_dry_run(&self, ltx_failed: bool) -> bool {
        !self.observed()
            && (!self.is_write
                || self.want_rows
                || ltx_failed
                || !matches!(self.outcome, Some(Outcome::Done(_))))
    }
    fn bytes(&self) -> usize {
        fn size(value: &Value) -> usize {
            match value {
                Value::Text(v) => v.len(),
                Value::Blob(v) => v.len(),
                Value::Null | Value::Integer(_) | Value::Real(_) => 8,
            }
        }
        self.sql.len()
            + match &self.params {
                SqlParams::None => 0,
                SqlParams::Positional(values) => values.iter().map(size).sum(),
                SqlParams::Named(values) => values.iter().map(|(n, v)| n.len() + size(v)).sum(),
            }
    }
    fn step(&self, condition: Option<Condition>) -> BatchStep {
        BatchStep {
            sql: self.sql.clone(),
            params: self.params.clone(),
            want_rows: self.want_rows,
            condition,
        }
    }
    fn record(&mut self, outcome: StepOutcome) {
        self.outcome = match outcome {
            StepOutcome::Done(result) => Some(Outcome::Done(Arc::new(result))),
            StepOutcome::Error { code, message } => Some(Outcome::Error { code, message }),
            StepOutcome::Skipped => unreachable!("skipped steps are not recorded"),
        };
        self.observed_under = Some(self.prefix);
    }
}

#[derive(Default)]
struct LtxLog {
    entries: Vec<Entry>,
    cursor: usize,
    stopped: bool,
    failed: bool,
    start_prefix: Prefix,
}

#[derive(Default)]
struct State {
    ltxs: Vec<LtxLog>,
    current: usize,
    prefix: Prefix,
    epoch: Option<i64>,
    rounds: u32,
    epoch_conflicts: u32,
    transport_failures: u32,
    /// The last affected row count per SQL text makes speculation mostly right.
    hints: HashMap<String, usize>,
}

fn remember_affected(hints: &mut HashMap<String, usize>, sql: &str, affected: usize) {
    if hints.len() >= MAX_HINTS {
        hints.clear();
    }
    hints.insert(sql.to_owned(), affected);
}

/// How a chunk of LTXes ended.
pub(crate) struct Chunk {
    pub(crate) ltx_count: usize,
    pub(crate) result: Result<()>,
}

enum RoundEnd {
    Rerun,
    Done,
    Failed(rusqlite::Error),
    /// Retry with fewer LTXes, as the batch is too large or they keep invalidating each other.
    Split,
}

pub struct Replay {
    pipeline: Box<dyn SqlPipeline>,
    state: RefCell<State>,
}

fn not_autocommit() -> Condition {
    Condition::Not(Box::new(Condition::IsAutocommit))
}
fn plain(sql: impl Into<String>, condition: Option<Condition>, want_rows: bool) -> BatchStep {
    BatchStep {
        sql: sql.into(),
        params: SqlParams::None,
        want_rows,
        condition,
    }
}
fn hash_write(prefix: &Prefix, sql: &str, params: &SqlParams) -> Prefix {
    fn value(hash: &mut Sha256, value: &Value) {
        let (tag, bytes): (u8, Vec<u8>) = match value {
            Value::Null => (0, Vec::new()),
            Value::Integer(v) => (1, v.to_le_bytes().to_vec()),
            Value::Real(v) => (2, v.to_le_bytes().to_vec()),
            Value::Text(v) => (3, v.clone().into_bytes()),
            Value::Blob(v) => (4, v.clone()),
        };
        hash.update([tag]);
        hash.update(bytes.len().to_le_bytes());
        hash.update(bytes);
    }
    let mut hash = Sha256::new();
    hash.update(prefix);
    hash.update(sql.len().to_le_bytes());
    hash.update(sql);
    match params {
        SqlParams::None => {}
        SqlParams::Positional(values) => values.iter().for_each(|v| value(&mut hash, v)),
        SqlParams::Named(values) => values.iter().for_each(|(name, v)| {
            hash.update(name.len().to_le_bytes());
            hash.update(name);
            value(&mut hash, v);
        }),
    }
    hash.finalize().into()
}
fn is_transient(code: &str) -> bool {
    code.starts_with("SQLITE_BUSY") || code.starts_with("SQLITE_LOCKED")
}
fn single_integer(outcome: &StepOutcome) -> Option<i64> {
    if let StepOutcome::Done(result) = outcome
        && let [Value::Integer(value)] = result.rows.first()?.as_slice()
    {
        Some(*value)
    } else {
        None
    }
}
fn backoff(attempt: u32) {
    let base = 5 * u64::from(attempt.min(20));
    // Jitter grows with the delay so that conflicting nodes stop retrying in lockstep.
    std::thread::sleep(Duration::from_millis(
        base + rand::random_range(0..=base + 10),
    ));
}

impl Replay {
    #[must_use]
    pub fn new(pipeline: Box<dyn SqlPipeline>) -> Self {
        Self {
            pipeline,
            state: RefCell::default(),
        }
    }

    pub(crate) fn execute(&self, sql: &str, params: SqlParams) -> Result<usize> {
        Ok(self.statement(sql, params, false)?.affected)
    }

    pub(crate) fn query(&self, sql: &str, params: SqlParams) -> Result<Arc<QueryResult>> {
        self.statement(sql, params, true)
    }

    fn statement(&self, sql: &str, params: SqlParams, want_rows: bool) -> Result<Arc<QueryResult>> {
        let state = &mut *self.state.borrow_mut();
        let ltx = &mut state.ltxs[state.current];
        if ltx.stopped {
            return Err(pending());
        }
        let is_write = !want_rows || !is_read_only(sql);
        let index = ltx.cursor;
        ltx.cursor += 1;
        if !ltx.entries.get(index).is_some_and(|entry| {
            entry.sql == sql && entry.want_rows == want_rows && entry.params == params
        }) {
            ltx.entries.truncate(index);
            ltx.entries.push(Entry {
                sql: sql.to_owned(),
                params,
                is_write,
                want_rows,
                outcome: None,
                prefix: Prefix::default(),
                observed_under: None,
            });
        }
        let entry = &mut ltx.entries[index];
        entry.prefix = state.prefix;
        if is_write {
            state.prefix = hash_write(&state.prefix, sql, &entry.params);
        }
        if entry.outcome.is_none() && !want_rows {
            entry.outcome = Some(Outcome::Done(Arc::new(QueryResult {
                affected: state.hints.get(sql).copied().unwrap_or(1),
                ..QueryResult::default()
            })));
        }
        if let Some(outcome) = &entry.outcome {
            outcome.to_result()
        } else {
            ltx.stopped = true;
            Err(pending())
        }
    }

    /// Runs one physical transaction over a prefix of `ltxs`, which the caller repeats for the
    /// rest. `apply` runs one closure and reports its failure. Returns `None` on shutdown.
    pub(crate) fn run_chunk<T>(
        &self,
        ltxs: &mut [T],
        shutdown_requested: &dyn Fn() -> bool,
        mut apply: impl FnMut(&mut T) -> bool,
    ) -> Option<Chunk> {
        let mut ltx_count = ltxs.len();
        self.reset(ltx_count);
        loop {
            if shutdown_requested() {
                return None;
            }
            {
                let mut state = self.state.borrow_mut();
                state.prefix = Prefix::default();
                for ltx in &mut state.ltxs {
                    ltx.cursor = 0;
                    ltx.stopped = false;
                    ltx.failed = false;
                }
            }
            for (index, ltx) in ltxs[..ltx_count].iter_mut().enumerate() {
                {
                    let mut state = self.state.borrow_mut();
                    state.current = index;
                    state.ltxs[index].start_prefix = state.prefix;
                }
                let failed = apply(ltx);
                let mut state = self.state.borrow_mut();
                let state = &mut *state;
                let log = &mut state.ltxs[index];
                log.entries.truncate(log.cursor);
                log.failed = failed && !log.stopped;
                if log.failed {
                    state.prefix = log.start_prefix;
                }
            }
            match self.finish_round() {
                RoundEnd::Rerun => {}
                RoundEnd::Done => {
                    return Some(Chunk {
                        ltx_count,
                        result: Ok(()),
                    });
                }
                RoundEnd::Failed(error) => {
                    return Some(Chunk {
                        ltx_count,
                        result: Err(error),
                    });
                }
                RoundEnd::Split => {
                    ltx_count = ltx_count.div_ceil(2);
                    debug!(ltx_count, "splitting a remote batch");
                    self.reset(ltx_count);
                }
            }
        }
    }

    fn reset(&self, ltx_count: usize) {
        let mut state = self.state.borrow_mut();
        state.ltxs.clear();
        state.ltxs.resize_with(ltx_count, LtxLog::default);
        state.epoch = None;
        state.rounds = 0;
        state.epoch_conflicts = 0;
        state.transport_failures = 0;
    }

    fn finish_round(&self) -> RoundEnd {
        let state = &mut *self.state.borrow_mut();
        state.rounds += 1;
        if state.ltxs.len() > 1 && state.rounds > MAX_ROUNDS_BEFORE_SPLIT {
            return RoundEnd::Split;
        }
        if state.rounds > MAX_ROUNDS {
            return RoundEnd::Failed(replay_error(format!(
                "remote transaction did not converge in {MAX_ROUNDS} rounds"
            )));
        }
        let entries = || state.ltxs.iter().flat_map(|ltx| &ltx.entries);
        if state.ltxs.len() > 1 && entries().map(Entry::bytes).sum::<usize>() > MAX_BATCH_BYTES {
            return RoundEnd::Split;
        }
        let dry_run = state.ltxs.iter().any(|ltx| {
            ltx.entries
                .iter()
                .any(|entry| entry.needs_dry_run(ltx.failed))
        });
        if dry_run {
            self.dry_run(state)
        } else if state
            .ltxs
            .iter()
            .any(|ltx| !ltx.failed && ltx.entries.iter().any(|entry| entry.is_write))
        {
            self.commit(state)
        } else {
            // All reads come from one epoch and no write is pending.
            RoundEnd::Done
        }
    }

    fn dry_run(&self, state: &mut State) -> RoundEnd {
        let has_writes = state
            .ltxs
            .iter()
            .any(|ltx| ltx.entries.iter().any(|entry| entry.is_write));
        let begin = if has_writes {
            "BEGIN IMMEDIATE"
        } else {
            "BEGIN"
        };
        let mut steps = vec![
            plain(begin, None, false),
            plain(EPOCH_READ, Some(not_autocommit()), true),
        ];
        let mut sent = Vec::new();
        for (ltx_index, ltx) in state.ltxs.iter().enumerate() {
            let savepoint = ltx.failed && ltx.entries.iter().any(|entry| entry.is_write);
            if savepoint {
                steps.push(plain("SAVEPOINT ltx", Some(not_autocommit()), false));
            }
            for (entry_index, entry) in ltx.entries.iter().enumerate() {
                if entry.is_write || !entry.observed() {
                    sent.push((steps.len(), ltx_index, entry_index));
                    steps.push(entry.step(Some(not_autocommit())));
                }
            }
            if savepoint {
                steps.push(plain("ROLLBACK TO ltx", Some(not_autocommit()), false));
                steps.push(plain("RELEASE ltx", Some(not_autocommit()), false));
            }
        }
        steps.push(plain("ROLLBACK", Some(not_autocommit()), false));
        let mut outcomes = match self.send(state, &steps) {
            Ok(outcomes) => outcomes,
            Err(end) => return end,
        };
        if let Some(end) = batch_start_problem(&outcomes[0]) {
            return end;
        }
        // Outcomes of this batch already belong to the new epoch.
        if let Err(end) = Self::check_epoch(state, single_integer(&outcomes[1])) {
            return end;
        }
        // Retried batches must not poison the log with a lock timeout or a skipped step.
        let transient = sent.iter().any(|(step, _, _)| match &outcomes[*step] {
            StepOutcome::Done(_) => false,
            StepOutcome::Error { code, .. } => is_transient(code),
            StepOutcome::Skipped => true,
        });
        if transient {
            backoff(state.rounds);
            return RoundEnd::Rerun;
        }
        for (step, ltx_index, entry_index) in sent {
            let outcome = std::mem::replace(&mut outcomes[step], StepOutcome::Skipped);
            let entry = &mut state.ltxs[ltx_index].entries[entry_index];
            if entry.is_write
                && let StepOutcome::Done(result) = &outcome
            {
                remember_affected(&mut state.hints, &entry.sql, result.affected);
            }
            entry.record(outcome);
        }
        RoundEnd::Rerun
    }

    fn commit(&self, state: &mut State) -> RoundEnd {
        let mut steps = vec![
            plain("BEGIN IMMEDIATE", None, false),
            plain(EPOCH_READ, Some(not_autocommit()), true),
        ];
        let mut expected = vec![Condition::Ok(0)];
        if let Some(epoch) = state.epoch {
            expected.push(Condition::Ok(steps.len()));
            steps.push(plain(
                format!("SELECT CASE WHEN ({EPOCH_READ}) = {epoch} THEN 1 ELSE abs(-9223372036854775808) END"),
                Some(not_autocommit()),
                false,
            ));
        }
        let mut sent = Vec::new();
        // A failed LTX has no effect: its writes roll back and its reads were validated.
        for (ltx_index, ltx) in state.ltxs.iter().enumerate().filter(|(_, ltx)| !ltx.failed) {
            for (entry_index, entry) in ltx.entries.iter().enumerate() {
                if entry.is_write {
                    let step = steps.len();
                    sent.push((step, ltx_index, entry_index));
                    steps.push(entry.step(Some(not_autocommit())));
                    match &entry.outcome {
                        Some(Outcome::Done(result)) => {
                            expected.push(Condition::Ok(step));
                            if !entry.observed() {
                                expected.push(Condition::Ok(steps.len()));
                                steps.push(plain(
                                    format!(
                                        "SELECT CASE WHEN changes() = {} THEN 1 ELSE abs(-9223372036854775808) END",
                                        result.affected
                                    ),
                                    Some(not_autocommit()),
                                    false,
                                ));
                            }
                        }
                        _ => expected.push(Condition::Not(Box::new(Condition::Ok(step)))),
                    }
                }
            }
        }
        expected.push(Condition::Ok(steps.len()));
        steps.push(plain(EPOCH_BUMP, Some(not_autocommit()), false));
        let commit_step = steps.len();
        steps.push(plain("COMMIT", Some(Condition::And(expected)), false));
        steps.push(plain("ROLLBACK", Some(not_autocommit()), false));
        let mut outcomes = match self.send(state, &steps) {
            Ok(outcomes) => outcomes,
            Err(RoundEnd::Rerun | RoundEnd::Split) => {
                return RoundEnd::Failed(replay_error(
                    "remote commit failed, its outcome is unknown".to_string(),
                ));
            }
            Err(end) => return end,
        };
        match &outcomes[commit_step] {
            StepOutcome::Done(_) => return RoundEnd::Done,
            StepOutcome::Skipped => {}
            StepOutcome::Error { code, .. } if is_transient(code) => {}
            StepOutcome::Error { code, message } => {
                warn!(code, message, "remote commit rejected");
                return RoundEnd::Failed(replay_error(format!(
                    "remote commit rejected: {message}"
                )));
            }
        }
        if let Some(end) = batch_start_problem(&outcomes[0]) {
            return end;
        }
        match Self::check_epoch(state, single_integer(&outcomes[1])) {
            Ok(false) => {}
            Ok(true) => return RoundEnd::Rerun,
            Err(end) => return end,
        }
        // Learn the true row counts and constraint outcomes, the next round re-runs the closures.
        let mut learned = false;
        for (step, ltx_index, entry_index) in sent {
            let outcome = std::mem::replace(&mut outcomes[step], StepOutcome::Skipped);
            let usable = match &outcome {
                StepOutcome::Done(_) => true,
                StepOutcome::Error { code, .. } => !is_transient(code),
                StepOutcome::Skipped => false,
            };
            if usable {
                let entry = &mut state.ltxs[ltx_index].entries[entry_index];
                if let StepOutcome::Done(result) = &outcome {
                    remember_affected(&mut state.hints, &entry.sql, result.affected);
                }
                learned |= !entry.observed();
                entry.record(outcome);
            }
        }
        if !learned {
            backoff(state.rounds);
        }
        RoundEnd::Rerun
    }

    fn send(
        &self,
        state: &mut State,
        steps: &[BatchStep],
    ) -> std::result::Result<Vec<StepOutcome>, RoundEnd> {
        match self.pipeline.batch(steps) {
            Ok(outcomes) if outcomes.len() == steps.len() => {
                state.transport_failures = 0;
                Ok(outcomes)
            }
            Ok(outcomes) => Err(RoundEnd::Failed(replay_error(format!(
                "remote batch returned {} results for {} steps",
                outcomes.len(),
                steps.len()
            )))),
            Err(PipelineError::ResponseTooLarge) if state.ltxs.len() > 1 => Err(RoundEnd::Split),
            Err(error @ PipelineError::ResponseTooLarge) => {
                Err(RoundEnd::Failed(replay_error(error.to_string())))
            }
            Err(PipelineError::Failed(error)) => {
                state.transport_failures += 1;
                warn!(%error, failures = state.transport_failures, "remote SQL batch failed");
                if state.transport_failures >= MAX_CONSECUTIVE_TRANSPORT_FAILURES {
                    return Err(RoundEnd::Failed(replay_error(error)));
                }
                backoff(state.transport_failures * 4);
                Err(RoundEnd::Rerun)
            }
        }
    }

    /// Returns whether the epoch moved, which means another node committed. No known outcome
    /// holds then until a dry run observes it again.
    fn check_epoch(state: &mut State, seen: Option<i64>) -> std::result::Result<bool, RoundEnd> {
        let Some(seen) = seen else {
            return Err(RoundEnd::Failed(replay_error(
                "remote batch did not return the write epoch".to_string(),
            )));
        };
        match state.epoch.replace(seen) {
            Some(known) if known != seen => {
                debug!(known, seen, "remote write epoch moved");
                for entry in state.ltxs.iter_mut().flat_map(|ltx| &mut ltx.entries) {
                    entry.observed_under = None;
                }
                state.rounds = 0;
                state.epoch_conflicts += 1;
                if state.epoch_conflicts > MAX_EPOCH_CONFLICTS {
                    return Err(RoundEnd::Failed(replay_error(format!(
                        "remote transaction lost to other writers {MAX_EPOCH_CONFLICTS} times"
                    ))));
                }
                backoff(state.epoch_conflicts);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

fn batch_start_problem(begin: &StepOutcome) -> Option<RoundEnd> {
    match begin {
        StepOutcome::Done(_) => None,
        StepOutcome::Error { code, .. } if is_transient(code) => {
            backoff(4);
            Some(RoundEnd::Rerun)
        }
        StepOutcome::Error { message, .. } => Some(RoundEnd::Failed(replay_error(format!(
            "cannot begin remote transaction: {message}"
        )))),
        StepOutcome::Skipped => Some(RoundEnd::Failed(replay_error(
            "remote transaction start was skipped".to_string(),
        ))),
    }
}

fn is_read_only(sql: &str) -> bool {
    let sql = sql.trim_start();
    let keyword = |word: &str| {
        sql.get(..word.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(word))
    };
    if keyword("SELECT") || keyword("VALUES") {
        return true;
    }
    // `PRAGMA x = 1` and `PRAGMA x(1)` can change state.
    if keyword("PRAGMA") {
        return !sql.contains(['=', '(']);
    }
    // A leading CTE can precede a write, which then must be replayed.
    keyword("WITH") && {
        let upper = sql.to_ascii_uppercase();
        ["INSERT ", "UPDATE ", "DELETE ", "REPLACE "]
            .iter()
            .all(|word| !upper.contains(word))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params_from_iter;
    use std::sync::{Arc, Mutex};

    type Hook = Box<dyn FnMut(&rusqlite::Connection, &[BatchStep]) -> Option<PipelineError> + Send>;

    /// Follows the Hrana batch semantics on a real engine, unfinished transactions end on close.
    struct LocalPipeline {
        conn: Arc<Mutex<rusqlite::Connection>>,
        batches: Arc<Mutex<Vec<Vec<String>>>>,
        hook: Mutex<Hook>,
        lose_commit_response: bool,
    }

    fn holds(condition: &Condition, conn: &rusqlite::Connection, done: &[StepOutcome]) -> bool {
        match condition {
            Condition::Ok(step) => matches!(done[*step], StepOutcome::Done(_)),
            Condition::Not(condition) => !holds(condition, conn, done),
            Condition::And(conditions) => conditions.iter().all(|c| holds(c, conn, done)),
            Condition::IsAutocommit => conn.is_autocommit(),
        }
    }

    fn run_step(conn: &rusqlite::Connection, step: &BatchStep) -> rusqlite::Result<QueryResult> {
        let mut statement = conn.prepare(&step.sql)?;
        let values = match &step.params {
            SqlParams::None => Vec::new(),
            SqlParams::Positional(values) => values.clone(),
            SqlParams::Named(_) => unreachable!("not used by these tests"),
        };
        let mut result = QueryResult::default();
        let column_count = statement.column_count();
        let mut rows = statement.query(params_from_iter(values))?;
        while let Some(row) = rows.next()? {
            result.rows.push(
                (0..column_count)
                    .map(|index| row.get::<_, Value>(index))
                    .collect::<rusqlite::Result<_>>()?,
            );
        }
        result.affected = usize::try_from(conn.changes()).unwrap();
        Ok(result)
    }

    impl SqlPipeline for LocalPipeline {
        fn batch(
            &self,
            steps: &[BatchStep],
        ) -> std::result::Result<Vec<StepOutcome>, PipelineError> {
            let conn = self.conn.lock().unwrap();
            self.batches
                .lock()
                .unwrap()
                .push(steps.iter().map(|step| step.sql.clone()).collect());
            if let Some(error) = (self.hook.lock().unwrap())(&conn, steps) {
                return Err(error);
            }
            let mut outcomes = Vec::with_capacity(steps.len());
            for step in steps {
                let run = step
                    .condition
                    .as_ref()
                    .is_none_or(|condition| holds(condition, &conn, &outcomes));
                outcomes.push(if run {
                    match run_step(&conn, step) {
                        Ok(result) => StepOutcome::Done(result),
                        Err(error) => StepOutcome::Error {
                            code: if error.sqlite_error_code()
                                == Some(rusqlite::ErrorCode::ConstraintViolation)
                            {
                                "SQLITE_CONSTRAINT".to_string()
                            } else {
                                "SQLITE_ERROR".to_string()
                            },
                            message: error.to_string(),
                        },
                    }
                } else {
                    StepOutcome::Skipped
                });
            }
            if !conn.is_autocommit() {
                conn.execute_batch("ROLLBACK").unwrap();
            }
            if self.lose_commit_response && steps.iter().any(|step| step.sql == "COMMIT") {
                return Err(PipelineError::Failed("connection reset".to_string()));
            }
            Ok(outcomes)
        }
    }

    struct Harness {
        replay: Replay,
        conn: Arc<Mutex<rusqlite::Connection>>,
        batches: Arc<Mutex<Vec<Vec<String>>>>,
    }

    fn harness_with(lose_commit_response: bool, hook: Hook) -> Harness {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(EPOCH_TABLE_SQL).unwrap();
        conn.execute_batch("CREATE TABLE kv (k TEXT PRIMARY KEY, v ANY)")
            .unwrap();
        let conn = Arc::new(Mutex::new(conn));
        let batches = Arc::new(Mutex::new(Vec::new()));
        Harness {
            replay: Replay::new(Box::new(LocalPipeline {
                conn: conn.clone(),
                batches: batches.clone(),
                hook: Mutex::new(hook),
                lose_commit_response,
            })),
            conn,
            batches,
        }
    }

    fn harness() -> Harness {
        harness_with(false, Box::new(|_, _| None))
    }

    type Ltx<'a> = Box<dyn FnMut(&Replay) -> Result<()> + 'a>;

    fn params(values: impl IntoIterator<Item = Value>) -> SqlParams {
        SqlParams::Positional(values.into_iter().collect())
    }
    fn text(value: impl Into<String>) -> Value {
        Value::Text(value.into())
    }
    fn get(replay: &Replay, key: &str) -> Result<Option<i64>> {
        let result = replay.query("SELECT v FROM kv WHERE k = ?1", params([text(key)]))?;
        Ok(result.rows.first().map(|row| match row[0] {
            Value::Integer(value) => value,
            ref other => panic!("unexpected value {other:?}"),
        }))
    }
    fn put(replay: &Replay, key: &str, value: i64) -> Result<usize> {
        replay.execute(
            "INSERT INTO kv (k, v) VALUES (?1, ?2) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
            params([text(key), Value::Integer(value)]),
        )
    }

    impl Harness {
        /// Commits the LTXes the way the worker does, returns each chunk's size and result.
        fn run(&self, ltxs: &mut [Ltx<'_>]) -> Vec<(usize, Result<()>)> {
            let mut chunks = Vec::new();
            let mut rest = ltxs;
            while !rest.is_empty() {
                let chunk = self
                    .replay
                    .run_chunk(rest, &|| false, |ltx| ltx(&self.replay).is_err())
                    .unwrap();
                chunks.push((chunk.ltx_count, chunk.result));
                rest = &mut rest[chunk.ltx_count..];
            }
            chunks
        }
        fn stored(&self, key: &str) -> Option<i64> {
            self.conn
                .lock()
                .unwrap()
                .query_row("SELECT v FROM kv WHERE k = ?1", [key], |row| row.get(0))
                .ok()
        }
        fn seed(&self, key: &str, value: i64) {
            self.conn
                .lock()
                .unwrap()
                .execute("INSERT INTO kv (k, v) VALUES (?1, ?2)", (key, value))
                .unwrap();
        }
        fn batch_count(&self) -> usize {
            self.batches.lock().unwrap().len()
        }
        fn commit_count(&self) -> usize {
            self.batches
                .lock()
                .unwrap()
                .iter()
                .filter(|batch| batch.iter().any(|sql| sql == "COMMIT"))
                .count()
        }
    }

    #[test]
    fn independent_read_write_ltxes_stack_into_three_batches() {
        const LTXES: i64 = 50;
        let harness = harness();
        (0..LTXES).for_each(|i| harness.seed(&format!("k{i}"), i));
        let mut ltxs: Vec<Ltx<'_>> = (0..LTXES)
            .map(|i| -> Ltx<'_> {
                Box::new(move |replay| {
                    let key = format!("k{i}");
                    let value = get(replay, &key)?.unwrap();
                    put(replay, &key, value + 1000)?;
                    Ok(())
                })
            })
            .collect();
        let chunks = harness.run(&mut ltxs);
        assert_eq!(usize::try_from(LTXES).unwrap(), chunks[0].0);
        chunks[0].1.as_ref().unwrap();
        (0..LTXES).for_each(|i| assert_eq!(Some(i + 1000), harness.stored(&format!("k{i}"))));
        // Reads, validation of the reads under the speculated writes, commit.
        assert_eq!(3, harness.batch_count());
        assert_eq!(1, harness.commit_count());
    }

    #[test]
    fn writes_with_correct_speculation_commit_in_one_batch() {
        let harness = harness();
        let mut ltxs: Vec<Ltx<'_>> = (0..20)
            .map(|i| -> Ltx<'_> {
                Box::new(move |replay| put(replay, &format!("k{i}"), i).map(|_| ()))
            })
            .collect();
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(19), harness.stored("k19"));
        assert_eq!(1, harness.batch_count());
    }

    #[test]
    fn dependent_ltxes_observe_their_predecessors_and_split_when_slow() {
        const LTXES: i64 = 40;
        let harness = harness();
        harness.seed("counter", 0);
        let seen = Mutex::new(vec![0; usize::try_from(LTXES).unwrap()]);
        let mut ltxs: Vec<Ltx<'_>> = (0..usize::try_from(LTXES).unwrap())
            .map(|i| -> Ltx<'_> {
                let seen = &seen;
                Box::new(move |replay| {
                    let value = get(replay, "counter")?.unwrap();
                    seen.lock().unwrap()[i] = value;
                    put(replay, "counter", value + 1)?;
                    Ok(())
                })
            })
            .collect();
        let chunks = harness.run(&mut ltxs);
        assert!(chunks.len() > 1, "{:?}", chunks.len());
        for (_, result) in &chunks {
            result.as_ref().unwrap();
        }
        assert_eq!(Some(LTXES), harness.stored("counter"));
        assert_eq!(
            (0..LTXES).collect::<Vec<_>>(),
            *seen.lock().unwrap(),
            "the committed order must be serial"
        );
    }

    #[test]
    fn wrong_affected_count_reruns_the_closure_with_the_true_count() {
        let harness = harness();
        let mut ltxs: Vec<Ltx<'_>> = vec![Box::new(|replay| {
            let updated = replay.execute(
                "UPDATE kv SET v = 1 WHERE k = ?1",
                params([text("missing")]),
            )?;
            if updated == 0 {
                put(replay, "missing", 2)?;
            }
            Ok(())
        })];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(2), harness.stored("missing"));
        assert_eq!(2, harness.commit_count(), "the guard rejects the first one");
        assert_eq!(2, harness.batch_count());
    }

    #[test]
    fn failed_ltx_leaves_nothing_behind_and_later_ltxes_do_not_see_it() {
        let harness = harness();
        let seen = Mutex::new(Some(-1));
        let mut ltxs: Vec<Ltx<'_>> = vec![
            Box::new(|replay| {
                put(replay, "failed", 1)?;
                Err(rusqlite::Error::InvalidQuery)
            }),
            Box::new(|replay| {
                *seen.lock().unwrap() = get(replay, "failed")?;
                put(replay, "after", 1)?;
                Ok(())
            }),
        ];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(None, *seen.lock().unwrap());
        assert_eq!(None, harness.stored("failed"));
        assert_eq!(Some(1), harness.stored("after"));
    }

    #[test]
    fn constraint_error_handled_by_the_closure_does_not_abort_the_commit() {
        let harness = harness();
        harness.seed("taken", 1);
        let mut ltxs: Vec<Ltx<'_>> = vec![Box::new(|replay| {
            let inserted = replay.execute(
                "INSERT INTO kv (k, v) VALUES (?1, 5)",
                params([text("taken")]),
            );
            match inserted {
                Err(error)
                    if error.sqlite_error_code()
                        == Some(rusqlite::ErrorCode::ConstraintViolation) =>
                {
                    put(replay, "fallback", 7)?;
                    Ok(())
                }
                other => other.map(|_| ()),
            }
        })];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(1), harness.stored("taken"));
        assert_eq!(Some(7), harness.stored("fallback"));
    }

    #[test]
    fn commit_by_another_node_restarts_with_fresh_reads() {
        let mut batch = 0;
        let harness = harness_with(
            false,
            Box::new(move |conn, _| {
                batch += 1;
                // Between the reads and the commit of this node.
                if batch == 3 {
                    conn.execute_batch(&format!("UPDATE kv SET v = 100; {EPOCH_BUMP};"))
                        .unwrap();
                }
                None
            }),
        );
        harness.seed("k", 1);
        let mut ltxs: Vec<Ltx<'_>> = vec![
            Box::new(|replay| {
                let value = get(replay, "k")?.unwrap();
                put(replay, "k", value + 1)?;
                Ok(())
            }),
            Box::new(|replay| {
                let value = get(replay, "k")?.unwrap();
                put(replay, "copy", value)?;
                Ok(())
            }),
        ];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(101), harness.stored("k"));
        assert_eq!(Some(101), harness.stored("copy"));
    }

    #[test]
    fn epoch_conflict_revalidates_known_reads_in_one_batch() {
        let mut batch = 0;
        let harness = harness_with(
            false,
            Box::new(move |conn, _| {
                batch += 1;
                if batch == 2 {
                    conn.execute_batch(&format!(
                        "UPDATE kv SET v = 100 WHERE k = 'a'; {EPOCH_BUMP};"
                    ))
                    .unwrap();
                }
                None
            }),
        );
        harness.seed("a", 1);
        harness.seed("b", 2);
        harness.seed("c", 3);
        let mut ltxs: Vec<Ltx<'_>> = vec![Box::new(|replay| {
            let sum = get(replay, "a")?.unwrap()
                + get(replay, "b")?.unwrap()
                + get(replay, "c")?.unwrap();
            put(replay, "sum", sum)?;
            Ok(())
        })];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(105), harness.stored("sum"));
        // The read of `a` is sent along with `c`, the conflict costs no extra round.
        assert_eq!(4, harness.batch_count());
    }

    #[test]
    fn epoch_conflicts_do_not_use_up_the_rounds_of_a_stacked_chunk() {
        let mut batch = 0;
        let harness = harness_with(
            false,
            Box::new(move |conn, _| {
                batch += 1;
                if batch <= MAX_ROUNDS_BEFORE_SPLIT + 4 {
                    conn.execute_batch(EPOCH_BUMP).unwrap();
                }
                None
            }),
        );
        harness.seed("a", 1);
        let mut ltxs: Vec<Ltx<'_>> = vec![
            Box::new(|replay| {
                let value = get(replay, "a")?.unwrap();
                put(replay, "b", value)?;
                Ok(())
            }),
            Box::new(|replay| {
                let value = get(replay, "a")?.unwrap();
                put(replay, "c", value)?;
                Ok(())
            }),
        ];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(1), harness.stored("c"));
        assert_eq!(1, harness.commit_count());
    }

    #[test]
    fn lost_commit_response_is_reported_without_a_second_commit() {
        let harness = harness_with(true, Box::new(|_, _| None));
        harness.seed("counter", 0);
        let mut ltxs: Vec<Ltx<'_>> = vec![Box::new(|replay| {
            let value = get(replay, "counter")?.unwrap();
            put(replay, "counter", value + 1)?;
            Ok(())
        })];
        assert!(harness.run(&mut ltxs)[0].1.is_err());
        assert_eq!(Some(1), harness.stored("counter"));
        assert_eq!(1, harness.commit_count());
    }

    #[test]
    fn lost_dry_run_response_is_retried() {
        let mut failures = 2;
        let harness = harness_with(
            false,
            Box::new(move |_, _| {
                (failures > 0).then(|| {
                    failures -= 1;
                    PipelineError::Failed("connection reset".to_string())
                })
            }),
        );
        harness.seed("k", 1);
        let mut ltxs: Vec<Ltx<'_>> = vec![Box::new(|replay| {
            let value = get(replay, "k")?.unwrap();
            put(replay, "k", value + 1)?;
            Ok(())
        })];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(2), harness.stored("k"));
    }

    #[test]
    fn too_large_response_splits_the_ltxes_until_a_single_one_fails() {
        // Allows two reads per batch, or one read of the key that never fits.
        let harness = harness_with(
            false,
            Box::new(|_, steps| {
                let reads = || steps.iter().filter(|step| step.sql.starts_with("SELECT v"));
                let too_large = reads().count() > 2
                    || reads().any(|step| step.params == params([text("huge")]));
                too_large.then_some(PipelineError::ResponseTooLarge)
            }),
        );
        let read =
            |key: &'static str| -> Ltx<'_> { Box::new(move |replay| get(replay, key).map(|_| ())) };
        let mut ltxs = vec![read("a"), read("b"), read("c"), read("d"), read("huge")];
        let chunks = harness.run(&mut ltxs);
        let sizes = chunks.iter().map(|(size, _)| *size).collect::<Vec<_>>();
        assert_eq!(vec![2, 2, 1], sizes);
        chunks[0].1.as_ref().unwrap();
        chunks[1].1.as_ref().unwrap();
        assert!(chunks[2].1.is_err());
    }

    #[test]
    fn large_requests_split_by_bytes() {
        let harness = harness();
        let blob = || Value::Blob(vec![0; MAX_BATCH_BYTES * 2 / 3]);
        let insert = |key: &'static str| -> Ltx<'_> {
            Box::new(move |replay| {
                replay
                    .execute(
                        "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                        params([text(key), blob()]),
                    )
                    .map(|_| ())
            })
        };
        let mut ltxs = vec![insert("a"), insert("b")];
        let chunks = harness.run(&mut ltxs);
        assert_eq!(2, chunks.len());
        assert_eq!(2, harness.commit_count());
    }

    #[test]
    fn read_only_ltxes_never_commit() {
        let harness = harness();
        harness.seed("k", 1);
        let seen = Mutex::new(None);
        let mut ltxs: Vec<Ltx<'_>> = vec![Box::new(|replay| {
            *seen.lock().unwrap() = get(replay, "k")?;
            replay.query("PRAGMA page_count", SqlParams::None)?;
            Ok(())
        })];
        harness.run(&mut ltxs)[0].1.as_ref().unwrap();
        assert_eq!(Some(1), *seen.lock().unwrap());
        assert_eq!(2, harness.batch_count());
        assert_eq!(0, harness.commit_count());
    }
}
