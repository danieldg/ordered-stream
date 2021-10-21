#![no_std]
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::{FusedStream, Stream};

/// A stream that can be joined with other streams while preserving a element ordering.
///
/// Say you have a bunch of events that all have a timestamp, sequence number, or other ordering
/// attribute.  Also assume you are getting these events from multiple [`Stream`]s.  If each stream
/// is ordered, it should be possible to produce a "composite" stream by joining each of the
/// individual streams.
///
/// However, if you actually implement this, you discover that you need to buffer at least one
/// element from each stream in order to avoid ordering inversions if the sources are independent
/// (including just running in different tasks).  This presents a problem if one of the sources
/// rarely produces events: that slow source can stall all other streams in order to handle the
/// case where the slowness is due to an earlier element instead of just having no elements.
///
/// This trait provides a way to solve this problem: if you can ask a stream if it will ever have
/// any events older than a certain value, then you can avoid blocking the composite stream.
pub trait JoinableStream: Stream {
    type OrderingToken: Ord;

    /// Attempt to pull out the next value of this stream, registering the current task for wakeup
    /// if needed, and returning `None` if it is known that the stream will not produce any more
    /// values ordered before the given token.
    ///
    /// Calls to [`Stream::poll_next`] are equivalent to calls to `poll_next_before` specifying
    /// `None` as the value of `before`.  In particular, the termination state of the stream is
    /// shared between these two traits.
    ///
    /// # Return value
    ///
    /// There are several possible return values, each indicating a distinct stream state depending
    /// on the value passed in `before`:
    ///
    /// - If `before` was `None`, `Poll::Pending` means that this stream's next value is not ready
    /// yet. Implementations will ensure that the current task is notified when the next value may
    /// be ready.
    ///
    /// - If `before` was `Some`, `Poll::Pending` means that this stream's next value is not ready
    /// and that it is not yet known if the stream will produce a value ordered prior to the given
    /// ordering token.  Implementations will ensure that the current task is notified when either
    /// the next value is ready or once it is known that no such value will be produced.
    ///
    /// - `Poll::Ready(JoinableStreamResult::Item)` means that the stream has successfully produced
    /// an item and associated ordering token.  The stream may produce further values on subsequent
    /// `poll_next_token` calls.  The returned ordering token **may** be greater than the value
    /// passed to `before`.
    ///
    /// - `Poll::Ready(JoinableStreamResult::Terminated)` means that the stream has terminated, and
    /// `poll_next_token` or `poll_next` should not be invoked again.
    ///
    /// - `Poll::Ready(JoinableStreamResult::NoneBefore)` means that the stream will not produce
    /// any further ordering tokens less than the given token.  Subsequent `poll_next_token` calls
    /// may still produce additional items, but their tokens will be greater than or equal to the
    /// given token.  It does not make sense to return this value if `before` was `None`.
    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>>;
}

/// The result of a [`JoinableStream::poll_next_before`] operation.
#[derive(Debug)]
pub enum JoinableStreamResult<Item, Token> {
    /// An item with a corresponding ordering token.
    Item { item: Item, token: Token },
    /// This stream will not return any items prior to the given point.
    NoneBefore,
    /// This stream is terminated and should not be polled again.
    Terminated,
}

impl<I, T> JoinableStreamResult<I, T> {
    /// Extract the item from the result.
    pub fn into_item(self) -> Option<I> {
        match self {
            Self::Item { item, .. } => Some(item),
            _ => None,
        }
    }

    /// Apply a closure to the item.
    pub fn map_item<R>(self, f: impl FnOnce(I) -> R) -> JoinableStreamResult<R, T> {
        match self {
            Self::Item { item, token } => JoinableStreamResult::Item {
                item: f(item),
                token,
            },
            Self::NoneBefore => JoinableStreamResult::NoneBefore,
            Self::Terminated => JoinableStreamResult::Terminated,
        }
    }
}

pin_project_lite::pin_project! {
    /// A stream for the [`into_blocking_joinable`] function.
    #[derive(Debug)]
    pub struct IntoBlockingJoinable<S, F> {
        #[pin]
        stream: S,
        get_token: F,
    }
}

/// Convert a [`Stream`] into a [`JoinableStream`] without using any future or past knowledge of
/// elements.
///
/// This is suitable if the stream rarely or never blocks.  In general, prefer using
/// [`into_joinable`] as it will return [`JoinableStreamResult::NoneBefore`] when possible.
pub fn into_blocking_joinable<S, F, T>(stream: S, get_token: F) -> IntoBlockingJoinable<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> T,
    T: Ord,
{
    IntoBlockingJoinable { stream, get_token }
}

impl<S, F> Stream for IntoBlockingJoinable<S, F>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        self.project().stream.poll_next(cx)
    }
}

impl<S, F, T> JoinableStream for IntoBlockingJoinable<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> T,
    T: Ord,
{
    type OrderingToken = T;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>> {
        let this = self.project();
        let get_token = this.get_token;
        this.stream.poll_next(cx).map(|opt| match opt {
            None => JoinableStreamResult::Terminated,
            Some(item) => {
                let token = get_token(&item);
                JoinableStreamResult::Item { item, token }
            }
        })
    }
}

pin_project_lite::pin_project! {
    /// A stream for the [`into_joinable`] function.
    #[derive(Debug)]
    pub struct IntoJoinable<S, F, T> {
        #[pin]
        stream: S,
        get_token: F,
        last: Option<T>,
    }
}

/// Convert a [`Stream`] into a [`JoinableStream`] without using any future knowledge of elements.
pub fn into_joinable<S, F, T>(stream: S, get_token: F) -> IntoJoinable<S, F, T>
where
    S: Stream,
    F: FnMut(&mut S::Item) -> T,
    T: Ord + Clone,
{
    IntoJoinable {
        stream,
        get_token,
        last: None,
    }
}

impl<S, F, T> Stream for IntoJoinable<S, F, T>
where
    S: Stream,
    F: FnMut(&mut S::Item) -> T,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        let this = self.project();
        let get_token = this.get_token;
        let last = this.last;
        this.stream.poll_next(cx).map(|opt| {
            opt.map(|mut item| {
                *last = Some(get_token(&mut item));
                item
            })
        })
    }
}

impl<S, F, T> JoinableStream for IntoJoinable<S, F, T>
where
    S: Stream,
    F: FnMut(&mut S::Item) -> T,
    T: Ord + Clone,
{
    type OrderingToken = T;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>> {
        let this = self.project();
        let get_token = this.get_token;
        let last = this.last;
        if let (Some(last), Some(before)) = (last.as_ref(), before) {
            if last >= before {
                return Poll::Ready(JoinableStreamResult::NoneBefore);
            }
        }
        this.stream.poll_next(cx).map(|opt| match opt {
            None => JoinableStreamResult::Terminated,
            Some(mut item) => {
                let token = get_token(&mut item);
                *last = Some(token.clone());
                JoinableStreamResult::Item { item, token }
            }
        })
    }
}

pin_project_lite::pin_project! {
    /// A stream produced by the [`join`] method.
    #[derive(Debug)]
    pub struct StreamJoin<A, B>
    where
        A: JoinableStream,
        B: JoinableStream<Item = A::Item, OrderingToken=A::OrderingToken>,
    {
        #[pin]
        stream_a: A,
        #[pin]
        stream_b: B,
        state: JoinState<A::Item, B::Item, A::OrderingToken>,
    }
}

/// Join two streams while preserving the overall ordering of elements
pub fn join<A, B>(stream_a: A, stream_b: B) -> StreamJoin<A, B>
where
    A: JoinableStream,
    B: JoinableStream<Item = A::Item, OrderingToken = A::OrderingToken>,
{
    StreamJoin {
        stream_a,
        stream_b,
        state: JoinState::None,
    }
}

#[derive(Debug)]
enum JoinState<A, B, T> {
    None,
    A(A, T),
    B(B, T),
    OnlyPollA,
    OnlyPollB,
    Terminated,
}

impl<A, B, T> JoinState<A, B, T> {
    fn take_split(&mut self) -> (PollState<A, T>, PollState<B, T>) {
        match mem::replace(self, JoinState::None) {
            JoinState::None => (PollState::Pending, PollState::Pending),
            JoinState::A(a, t) => (PollState::Item(a, t), PollState::Pending),
            JoinState::B(b, t) => (PollState::Pending, PollState::Item(b, t)),
            JoinState::OnlyPollA => (PollState::Pending, PollState::Terminated),
            JoinState::OnlyPollB => (PollState::Terminated, PollState::Pending),
            JoinState::Terminated => (PollState::Terminated, PollState::Terminated),
        }
    }
}

impl<A, B> Stream for StreamJoin<A, B>
where
    A: JoinableStream,
    B: JoinableStream<Item = A::Item, OrderingToken = A::OrderingToken>,
{
    type Item = A::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<A::Item>> {
        self.poll_next_before(cx, None).map(|r| r.into_item())
    }
}

enum PollState<I, T> {
    Item(I, T),
    Pending,
    NoneBefore,
    Terminated,
}

impl<I, T: Ord> PollState<I, T> {
    fn token(&self) -> Option<&T> {
        match self {
            Self::Item(_, t) => Some(t),
            _ => None,
        }
    }

    fn update(
        &mut self,
        before: Option<&T>,
        other_token: Option<&T>,
        retry: bool,
        run: impl FnOnce(Option<&T>) -> Poll<JoinableStreamResult<I, T>>,
    ) -> bool {
        match self {
            // Do not re-poll if we have an item already or if we are terminated
            Self::Item { .. } | Self::Terminated => return false,

            // No need to re-poll if we already declared no items <= before
            Self::NoneBefore if retry => return false,

            _ => {}
        }

        // Run the poll with the earlier of the two tokens to avoid transitioning to Pending (which
        // will stall the StreamJoin) when we could have transitioned to NoneBefore.
        let token = match (before, other_token) {
            (Some(u), Some(o)) => {
                if *u > *o {
                    // The other token is earlier - so a retry might let us upgrade a Pending to a
                    // NoneBefore
                    Some(o)
                } else if retry {
                    // A retry will not improve matters, so don't bother
                    return false;
                } else {
                    Some(u)
                }
            }
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        };

        match run(token) {
            Poll::Ready(JoinableStreamResult::Item { item, token }) => {
                *self = Self::Item(item, token);
                true
            }
            Poll::Ready(JoinableStreamResult::NoneBefore) => {
                *self = Self::NoneBefore;
                false
            }
            Poll::Ready(JoinableStreamResult::Terminated) => {
                *self = Self::Terminated;
                false
            }
            Poll::Pending => {
                *self = Self::Pending;
                false
            }
        }
    }

    fn into_poll(self) -> Poll<JoinableStreamResult<I, T>> {
        match self {
            Self::Item(item, token) => Poll::Ready(JoinableStreamResult::Item { item, token }),
            Self::Pending => Poll::Pending,
            Self::NoneBefore => Poll::Ready(JoinableStreamResult::NoneBefore),
            Self::Terminated => Poll::Ready(JoinableStreamResult::Terminated),
        }
    }
}

impl<A, B> JoinableStream for StreamJoin<A, B>
where
    A: JoinableStream,
    B: JoinableStream<Item = A::Item, OrderingToken = A::OrderingToken>,
{
    type OrderingToken = A::OrderingToken;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>> {
        let mut this = self.project();
        let (mut poll_a, mut poll_b) = this.state.take_split();

        poll_a.update(before, poll_b.token(), false, |token| {
            this.stream_a.as_mut().poll_next_before(cx, token)
        });
        if poll_b.update(before, poll_a.token(), false, |token| {
            this.stream_b.as_mut().poll_next_before(cx, token)
        }) {
            // If B just got an item, it's possible that A already knows that it won't have any
            // items before that item; we couldn't ask that question before.  Ask it now.
            poll_a.update(before, poll_b.token(), true, |token| {
                this.stream_a.as_mut().poll_next_before(cx, token)
            });
        }

        match (poll_a, poll_b) {
            // Both are ready - we can judge ordering directly (simplest case).  The first one is
            // returned while the other one is buffered for the next poll.
            (PollState::Item(a, ta), PollState::Item(b, tb)) => {
                if ta <= tb {
                    *this.state = JoinState::B(b, tb);
                    Poll::Ready(JoinableStreamResult::Item { item: a, token: ta })
                } else {
                    *this.state = JoinState::A(a, ta);
                    Poll::Ready(JoinableStreamResult::Item { item: b, token: tb })
                }
            }

            // If both sides are terminated, so are we.
            (PollState::Terminated, PollState::Terminated) => {
                *this.state = JoinState::Terminated;
                Poll::Ready(JoinableStreamResult::Terminated)
            }

            // If one side is terminated, we can produce items directly from the other side.
            (a, PollState::Terminated) => {
                *this.state = JoinState::OnlyPollA;
                a.into_poll()
            }
            (PollState::Terminated, b) => {
                *this.state = JoinState::OnlyPollB;
                b.into_poll()
            }

            // If one side is pending, we can't return Ready until that gets resolved.  Because we
            // have already requested that our child streams wake us when it is possible to make
            // any kind of progress, we meet the requirements to return Poll::Pending.
            (PollState::Item(a, t), PollState::Pending) => {
                *this.state = JoinState::A(a, t);
                Poll::Pending
            }
            (PollState::Pending, PollState::Item(b, t)) => {
                *this.state = JoinState::B(b, t);
                Poll::Pending
            }
            (PollState::Pending, PollState::Pending) => Poll::Pending,
            (PollState::Pending, PollState::NoneBefore) => Poll::Pending,
            (PollState::NoneBefore, PollState::Pending) => Poll::Pending,

            // If both sides report NoneBefore, so can we.
            (PollState::NoneBefore, PollState::NoneBefore) => {
                Poll::Ready(JoinableStreamResult::NoneBefore)
            }

            (PollState::Item(item, token), PollState::NoneBefore) => {
                // B was polled using either the Some value of (before) or using A's token.
                //
                // If before is set and is earlier than A's token, then B might later produce a
                // value with (bt >= before && bt < at), so we can't return A's item yet and must
                // buffer it.  However, we can return None because neither stream will produce
                // items before the token passed in before.
                //
                // If before is either None or after A's token, B's NoneBefore return represents a
                // promise to not produce an item before A's, so we can return A's item now.
                match before {
                    Some(before) if token > *before => {
                        *this.state = JoinState::A(item, token);
                        Poll::Ready(JoinableStreamResult::NoneBefore)
                    }
                    _ => Poll::Ready(JoinableStreamResult::Item { item, token }),
                }
            }

            (PollState::NoneBefore, PollState::Item(item, token)) => {
                // A was polled using either the Some value of (before) or using B's token.
                //
                // By a mirror of the above argument, this NoneBefore result gives us permission to
                // produce either B's item or NoneBefore.
                match before {
                    Some(before) if token > *before => {
                        *this.state = JoinState::B(item, token);
                        Poll::Ready(JoinableStreamResult::NoneBefore)
                    }
                    _ => Poll::Ready(JoinableStreamResult::Item { item, token }),
                }
            }
        }
    }
}

impl<A, B> FusedStream for StreamJoin<A, B>
where
    A: JoinableStream,
    B: JoinableStream<Item = A::Item, OrderingToken = A::OrderingToken>,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, JoinState::Terminated)
    }
}

pin_project_lite::pin_project! {
    /// A stream for the [`map`] function.
    #[derive(Debug)]
    pub struct Map<S, F> {
        #[pin]
        stream: S,
        f: F,
    }
}

pub fn map<S, F, R>(stream: S, f: F) -> Map<S, F>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> R,
{
    Map { stream, f }
}

impl<S, F, R> Stream for Map<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> R,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<R>> {
        let this = self.project();
        let f = this.f;
        this.stream.poll_next(cx).map(|opt| opt.map(f))
    }
}

impl<S, F, R> JoinableStream for Map<S, F>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> R,
{
    type OrderingToken = S::OrderingToken;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>> {
        let this = self.project();
        let f = this.f;
        this.stream
            .poll_next_before(cx, before)
            .map(|res| res.map_item(f))
    }
}

pin_project_lite::pin_project! {
    /// A stream for the [`filter_map`] function.
    #[derive(Debug)]
    pub struct FilterMap<S, F> {
        #[pin]
        stream: S,
        filter: F,
    }
}

pub fn filter_map<S, F, R>(stream: S, filter: F) -> FilterMap<S, F>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> Option<R>,
{
    FilterMap { stream, filter }
}

pub fn filter<S, F>(
    stream: S,
    mut filter: impl FnMut(&S::Item) -> bool,
) -> FilterMap<S, impl FnMut(S::Item) -> Option<S::Item>>
where
    S: JoinableStream,
{
    filter_map(
        stream,
        move |item| if filter(&item) { Some(item) } else { None },
    )
}

impl<S, F, R> Stream for FilterMap<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Option<R>,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<R>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(item)) => match (this.filter)(item) {
                    Some(v) => return Poll::Ready(Some(v)),
                    None => continue,
                },
            }
        }
    }
}

impl<S, F, R> JoinableStream for FilterMap<S, F>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> Option<R>,
{
    type OrderingToken = S::OrderingToken;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next_before(cx, before) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(JoinableStreamResult::Terminated) => {
                    return Poll::Ready(JoinableStreamResult::Terminated)
                }
                Poll::Ready(JoinableStreamResult::NoneBefore) => {
                    return Poll::Ready(JoinableStreamResult::NoneBefore)
                }
                Poll::Ready(JoinableStreamResult::Item { item, token }) => {
                    match (this.filter)(item) {
                        Some(item) => {
                            return Poll::Ready(JoinableStreamResult::Item { item, token })
                        }
                        None => continue,
                    }
                }
            }
        }
    }
}

pin_project_lite::pin_project! {
    #[project = ThenProj]
    #[project_replace = ThenDone]
    #[derive(Debug)]
    enum ThenItem<Fut, T> {
        Running { #[pin] future: Fut, token: T },
        Idle,
    }
}

pin_project_lite::pin_project! {
    /// A stream for the [`then`] function.
    #[derive(Debug)]
    pub struct Then<S, F, Fut>
        where S: JoinableStream
    {
        #[pin]
        stream: S,
        then: F,
        #[pin]
        future: ThenItem<Fut, S::OrderingToken>,
    }
}

pub fn then<S, F, Fut>(stream: S, then: F) -> Then<S, F, Fut>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    Then {
        stream,
        then,
        future: ThenItem::Idle,
    }
}

impl<S, F, Fut> Stream for Then<S, F, Fut>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_before(cx, None).map(|item| item.into_item())
    }
}

impl<S, F, Fut> JoinableStream for Then<S, F, Fut>
where
    S: JoinableStream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    type OrderingToken = S::OrderingToken;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::OrderingToken>,
    ) -> Poll<JoinableStreamResult<Self::Item, Self::OrderingToken>> {
        let mut this = self.project();
        loop {
            if let ThenProj::Running { future, token } = this.future.as_mut().project() {
                // First try to make progress on the future, as we are likely to need it eventually
                if let Poll::Ready(item) = future.poll(cx) {
                    if let ThenDone::Running { token, .. } =
                        this.future.as_mut().project_replace(ThenItem::Idle)
                    {
                        return Poll::Ready(JoinableStreamResult::Item { item, token });
                    }
                } else if let Some(before) = before {
                    // Don't return Pending if asked about a point that we know is past.
                    if *token < *before {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(JoinableStreamResult::NoneBefore);
                    }
                } else {
                    return Poll::Pending;
                }
            }
            match this.stream.as_mut().poll_next_before(cx, before) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(JoinableStreamResult::Terminated) => {
                    return Poll::Ready(JoinableStreamResult::Terminated)
                }
                Poll::Ready(JoinableStreamResult::NoneBefore) => {
                    return Poll::Ready(JoinableStreamResult::NoneBefore)
                }
                Poll::Ready(JoinableStreamResult::Item { item, token }) => {
                    this.future.set(ThenItem::Running {
                        future: (this.then)(item),
                        token,
                    });
                }
            }
        }
    }
}
