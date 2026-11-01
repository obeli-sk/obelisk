use chrono::{DateTime, Utc};
use concepts::time::ClockFn;
use hyper::body::{Body, Frame, SizeHint};
use std::{
    future::Future as _,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::watch;

pub struct LimitedBody<B> {
    body: Option<B>,
    deadline: DateTime<Utc>,
    clock: Box<dyn ClockFn>,
    timer: Pin<Box<tokio::time::Sleep>>,
    expired: Option<watch::Sender<()>>,
}

impl<B> LimitedBody<B> {
    pub fn new(
        body: B,
        duration: Duration,
        clock: Box<dyn ClockFn>,
        expired: Option<watch::Sender<()>>,
    ) -> Self {
        Self {
            body: Some(body),
            deadline: clock.now() + duration,
            clock,
            timer: Box::pin(tokio::time::sleep(duration.min(Duration::from_secs(1)))),
            expired,
        }
    }
}

impl<B: Body + Unpin> Body for LimitedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.body.is_none() {
            return Poll::Ready(None);
        }
        let remaining = (self.deadline - self.clock.now())
            .to_std()
            .unwrap_or_default();
        if remaining.is_zero() {
            self.body.take();
            if let Some(expired) = self.expired.take() {
                let _ = expired.send(());
            }
            return Poll::Ready(None);
        }
        if self.timer.as_mut().poll(cx).is_ready() {
            // Monotonic clocks stop during suspension; recheck wall time at least once a second.
            self.timer
                .as_mut()
                .reset(tokio::time::Instant::now() + remaining.min(Duration::from_secs(1)));
            let _ = self.timer.as_mut().poll(cx);
        }
        let body = self.body.as_mut().unwrap();
        let frame = Pin::new(&mut *body).poll_frame(cx);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) || body.is_end_stream() {
            self.body.take();
            self.expired.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(Body::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        if self.is_end_stream() {
            SizeHint::with_exact(0)
        } else {
            SizeHint::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body::Bytes;
    use std::{
        future::poll_fn,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    use test_utils::sim_clock::SimClock;
    use tokio::sync::mpsc;

    struct TestBody {
        frames: mpsc::UnboundedReceiver<Bytes>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for TestBody {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    impl Body for TestBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            self.frames
                .poll_recv(cx)
                .map(|frame| frame.map(|bytes| Ok(Frame::data(bytes))))
        }
    }

    #[tokio::test]
    async fn stream_limit_checks_wall_time_before_each_chunk() {
        let clock = SimClock::default();
        let (sender, frames) = mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut body = LimitedBody::new(
            TestBody {
                frames,
                dropped: dropped.clone(),
            },
            Duration::from_secs(600),
            clock.clone_box(),
            None,
        );
        sender.send(Bytes::from_static(b"first")).unwrap();
        let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.into_data().unwrap(), "first");
        sender.send(Bytes::from_static(b"late")).unwrap();
        clock.move_time_forward(Duration::from_secs(601));
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        assert!(dropped.load(Ordering::Relaxed));
        assert!(body.is_end_stream());
    }

    #[tokio::test]
    async fn stream_limit_wakes_silent_body_after_wall_clock_jump() {
        let clock = SimClock::default();
        let (_sender, frames) = mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let (expired, mut expiry) = watch::channel(());
        let mut body = LimitedBody::new(
            TestBody {
                frames,
                dropped: dropped.clone(),
            },
            Duration::from_millis(10),
            clock.clone_box(),
            Some(expired),
        );
        let task =
            tokio::spawn(async move { poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!task.is_finished());
        clock.move_time_forward(Duration::from_secs(3600));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), task,)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        expiry.changed().await.unwrap();
        assert!(dropped.load(Ordering::Relaxed));
    }
}
