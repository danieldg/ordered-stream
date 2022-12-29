use crate::*;
use core::ops::DerefMut;
use core::pin::Pin;
use core::task::{Context, Poll};

fn poll_multiple<I, P, S>(
    streams: I,
    cx: &mut Context<'_>,
    before: Option<&S::Ordering>,
) -> Poll<PollResult<S::Ordering, S::Data>>
where
    I: IntoIterator<Item = Pin<P>>,
    P: DerefMut<Target = Peekable<S>>,
    S: OrderedStream,
{
    // The stream with the earliest item that is actually before the given point
    let mut best: Option<Pin<P>> = None;
    let mut has_data = false;
    let mut has_pending = false;
    for mut stream in streams {
        let best_before = best.as_ref().and_then(|p| p.item().map(|i| &i.0));
        let before = match (before, best_before) {
            (Some(a), Some(b)) if a < b => Some(a),
            (_, Some(b)) => Some(b),
            (a, None) => a,
        };
        match stream.as_mut().poll_peek_before(cx, before) {
            Poll::Pending => {
                has_pending = true;
            }
            Poll::Ready(PollResult::Terminated) => continue,
            Poll::Ready(PollResult::NoneBefore) => {
                has_data = true;
            }
            Poll::Ready(PollResult::Item { ordering, .. }) => match before {
                Some(max) if max < ordering => continue,
                _ => {
                    best = Some(stream);
                }
            },
        }
    }
    match best {
        None if has_data => Poll::Ready(PollResult::NoneBefore),
        None if has_pending => Poll::Pending,
        None => Poll::Ready(PollResult::Terminated),
        // This is guaranteed to return PollResult::Item
        Some(mut stream) => stream.as_mut().poll_next_before(cx, before),
    }
}

/// Join a collection of [`OrderedStream`]s.
///
/// This is similar to repeatedly using [`join()`] on all the streams in the contained collection.
/// It is not optimized to avoid polling streams that are not ready, so it works best if the number
/// of streams is relatively small.
//
// Unlike `FutureUnordered` or `SelectAll`, the ordering properties that this struct provides can
// easily require that all items in the collection be consulted before returning any item.  An
// example of such a situation is a series of streams that all generate timestamps (locally) for
// their items and only return `NoneBefore` for past timestamps.  If only one stream produces an
// item for each call to `JoinMultiple::poll_next_before`, that timestamp must be checked against
// every other stream, and no amount of preparatory work or hints will help this.
//
// On the other hand, if all streams provide a position hint that matches their next item, it is
// possible to build a priority queue to sort the streams and reduce the cost of a single poll from
// `n` to `log(n)`.  This does require maintaining a snapshot of the hints (so S::Ordering: Clone),
// and will significantly increase the worst-case workload, so it should be a distinct type.
#[derive(Debug, Default, Clone)]
pub struct JoinMultiple<C>(pub C);
impl<C> Unpin for JoinMultiple<C> {}

impl<C, S> OrderedStream for JoinMultiple<C>
where
    for<'a> &'a mut C: IntoIterator<Item = &'a mut Peekable<S>>,
    S: OrderedStream + Unpin,
{
    type Ordering = S::Ordering;
    type Data = S::Data;
    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&S::Ordering>,
    ) -> Poll<PollResult<S::Ordering, S::Data>> {
        poll_multiple(self.get_mut().0.into_iter().map(Pin::new), cx, before)
    }
}

impl<C, S> FusedOrderedStream for JoinMultiple<C>
where
    for<'a> &'a mut C: IntoIterator<Item = &'a mut Peekable<S>>,
    for<'a> &'a C: IntoIterator<Item = &'a Peekable<S>>,
    S: OrderedStream + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.0.into_iter().all(|peekable| peekable.is_terminated())
    }
}

pin_project_lite::pin_project! {
    /// Join a collection of pinned [`OrderedStream`]s.
    ///
    /// This is identical to [`JoinMultiple`], but accepts [`OrderedStream`]s that are not [`Unpin`] by
    /// requiring that the collection provide a pinned [`IntoIterator`] implementation.
    ///
    /// This is not a feature available in most `std` collections.  If you wish to use them, you
    /// should use `Box::pin` to make the stream [`Unpin`] before inserting it in the collection,
    /// and then use [`JoinMultiple`] on the resulting collection.
    #[derive(Debug,Default,Clone)]
    pub struct JoinMultiplePin<C> {
        #[pin]
        pub streams: C,
    }
}

impl<C> JoinMultiplePin<C> {
    pub fn as_pin_mut(self: Pin<&mut Self>) -> Pin<&mut C> {
        self.project().streams
    }
}

impl<C, S> OrderedStream for JoinMultiplePin<C>
where
    for<'a> Pin<&'a mut C>: IntoIterator<Item = Pin<&'a mut Peekable<S>>>,
    S: OrderedStream,
{
    type Ordering = S::Ordering;
    type Data = S::Data;
    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&S::Ordering>,
    ) -> Poll<PollResult<S::Ordering, S::Data>> {
        poll_multiple(self.as_pin_mut(), cx, before)
    }
}

#[cfg(test)]
mod test {
    extern crate alloc;

    use crate::FromStream;
    use crate::JoinMultiple;
    use crate::OrderedStreamExt;
    use alloc::boxed::Box;
    use alloc::vec::Vec;
    use core::pin::Pin;
    use futures_core::Stream;

    #[test]
    fn join_mutiple() {
        futures_executor::block_on(async {
            pub struct Message {
                serial: u32,
            }

            pub struct RemoteLogSource {
                stream: Pin<Box<dyn Stream<Item = Message>>>,
            }

            let mut logs = [
                RemoteLogSource {
                    stream: Box::pin(futures_util::stream::iter([
                        Message { serial: 1 },
                        Message { serial: 4 },
                        Message { serial: 5 },
                    ])),
                },
                RemoteLogSource {
                    stream: Box::pin(futures_util::stream::iter([
                        Message { serial: 2 },
                        Message { serial: 3 },
                        Message { serial: 6 },
                    ])),
                },
            ];
            let streams: Vec<_> = logs
                .iter_mut()
                .map(|s| FromStream::with_ordering(&mut s.stream, |m| m.serial).peekable())
                .collect();
            let mut joined = JoinMultiple(streams);
            for i in 0..6 {
                let msg = joined.next().await.unwrap();
                assert_eq!(msg.serial, i as u32 + 1);
            }
        });
    }
}
