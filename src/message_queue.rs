use parking_lot::{Mutex, RwLock};

use futures::channel::oneshot;

/// A message queue with persistent history that can be subscribed to.
///
/// Definitely non-optimal. This would ideally be a lockless mpmc queue.
#[derive(Default)]
pub struct MessageQueue<T> {
    messages: RwLock<Vec<T>>,
    message_listeners: Mutex<Vec<Listener<T>>>,
}

impl<T: Clone> MessageQueue<T> {
    pub fn new() -> MessageQueue<T> {
        MessageQueue {
            messages: RwLock::new(Vec::new()),
            message_listeners: Mutex::new(Vec::new()),
        }
    }

    pub fn push_messages(&self, new_messages: &[T]) {
        let mut message_listeners = self.message_listeners.lock();
        let mut messages = self.messages.write();
        messages.extend_from_slice(new_messages);

        let mut remaining_listeners = Vec::new();

        for listener in message_listeners.drain(..) {
            match fire_listener_if_ready(&messages, listener) {
                Ok(_) => {}
                Err(listener) => remaining_listeners.push(listener),
            }
        }

        // Without this annotation, Rust gets confused since the first argument
        // is a MutexGuard, but the second is a Vec.
        *message_listeners = remaining_listeners;
    }

    /// Subscribe to any messages occurring after the given message cursor.
    pub fn subscribe(&self, cursor: u32) -> oneshot::Receiver<(u32, Vec<T>)> {
        let (sender, receiver) = oneshot::channel();

        // Use the same lock order as push_messages. Checking history and
        // registering interest must be atomic with respect to publication.
        let mut message_listeners = self.message_listeners.lock();
        message_listeners.retain(|listener| !listener.sender.is_canceled());

        let listener = {
            let listener = Listener { sender, cursor };

            let messages = self.messages.read();

            match fire_listener_if_ready(&messages, listener) {
                Ok(_) => return receiver,
                Err(listener) => listener,
            }
        };

        message_listeners.push(listener);

        receiver
    }

    /// Subscribe to any messages being pushed into the queue.
    ///
    /// This method is only useful in tests. Non-test code should use subscribe
    /// instead.
    #[cfg(test)]
    #[allow(unused)]
    pub fn subscribe_any(&self) -> oneshot::Receiver<(u32, Vec<T>)> {
        let cursor = {
            let messages = self.messages.read();
            messages.len() as u32
        };

        self.subscribe(cursor)
    }

    pub fn cursor(&self) -> u32 {
        self.messages.read().len() as u32
    }
}

struct Listener<T> {
    sender: oneshot::Sender<(u32, Vec<T>)>,
    cursor: u32,
}

fn fire_listener_if_ready<T: Clone>(
    messages: &[T],
    listener: Listener<T>,
) -> Result<(), Listener<T>> {
    let current_cursor = messages.len() as u32;

    if listener.sender.is_canceled() {
        return Ok(());
    }

    if listener.cursor < current_cursor {
        let new_messages = messages[(listener.cursor as usize)..].to_vec();
        let _ = listener.sender.send((current_cursor, new_messages));
        Ok(())
    } else {
        Err(listener)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resumes_all_messages_after_cursor() {
        let queue = MessageQueue::new();
        queue.push_messages(&[1, 2]);
        let mut receiver = queue.subscribe(1);
        assert_eq!(receiver.try_recv().unwrap(), Some((2, vec![2])));
    }

    #[test]
    fn pending_subscriber_receives_next_update() {
        let queue = MessageQueue::new();
        let mut receiver = queue.subscribe(0);
        assert_eq!(receiver.try_recv().unwrap(), None);
        queue.push_messages(&[7]);
        assert_eq!(receiver.try_recv().unwrap(), Some((1, vec![7])));
    }

    #[test]
    fn disconnected_subscribers_do_not_accumulate_while_idle() {
        let queue = MessageQueue::<u8>::new();
        for _ in 0..1000 {
            drop(queue.subscribe(0));
        }
        let mut receiver = queue.subscribe(0);
        assert_eq!(queue.message_listeners.lock().len(), 1);
        queue.push_messages(&[9]);
        assert_eq!(receiver.try_recv().unwrap(), Some((1, vec![9])));
        assert!(queue.message_listeners.lock().is_empty());
    }

    #[test]
    fn racing_subscription_never_loses_a_publication() {
        use std::sync::{Arc, Barrier};
        let queue = Arc::new(MessageQueue::new());
        for value in 0..500 {
            let barrier = Arc::new(Barrier::new(2));
            std::thread::scope(|scope| {
                let handle = scope.spawn(|| {
                    barrier.wait();
                    queue.subscribe(value)
                });
                barrier.wait();
                queue.push_messages(&[value]);
                let mut receiver = handle.join().unwrap();
                assert_eq!(receiver.try_recv().unwrap(), Some((value + 1, vec![value])));
            });
        }
    }
}
