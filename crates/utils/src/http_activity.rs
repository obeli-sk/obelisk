use chrono::{DateTime, Utc};
use hyper::body::{Body, Frame, SizeHint};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Clone, Debug)]
pub struct HttpActivity(Arc<Mutex<Snapshot>>);

#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    pub in_flight: usize,
    pub last_finished_at: DateTime<Utc>,
    pub generation: u64,
}

impl Default for HttpActivity {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Snapshot {
            in_flight: 0,
            last_finished_at: Utc::now(),
            generation: 0,
        })))
    }
}

impl HttpActivity {
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        *self.0.lock().unwrap()
    }

    #[must_use]
    pub fn begin(&self) -> RequestGuard {
        let mut state = self.0.lock().unwrap();
        state.in_flight += 1;
        state.generation = state.generation.wrapping_add(1);
        RequestGuard(self.clone())
    }
}

pub struct RequestGuard(HttpActivity);

impl Drop for RequestGuard {
    fn drop(&mut self) {
        let mut state = self.0.0.lock().unwrap();
        state.in_flight -= 1;
        state.last_finished_at = Utc::now();
    }
}

pub struct TrackedBody<B> {
    body: B,
    guard: Option<RequestGuard>,
}

impl<B: Body> TrackedBody<B> {
    pub fn new(body: B, guard: RequestGuard) -> Self {
        let guard = (!body.is_end_stream()).then_some(guard);
        Self { body, guard }
    }
}

impl<B: Body + Unpin> Body for TrackedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) || self.body.is_end_stream() {
            self.guard.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
