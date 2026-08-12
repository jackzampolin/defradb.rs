//! Bounded channels backed by the native runtime or a WASM-compatible queue.

/// Sending side of a bounded channel.
pub struct Sender<T> {
    #[cfg(not(target_arch = "wasm32"))]
    inner: tokio::sync::mpsc::Sender<T>,
    #[cfg(target_arch = "wasm32")]
    inner: async_channel::Sender<T>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Send a value, returning it when the receiver is closed.
    pub async fn send(&self, value: T) -> Result<(), T> {
        #[cfg(not(target_arch = "wasm32"))]
        return self.inner.send(value).await.map_err(|error| error.0);

        #[cfg(target_arch = "wasm32")]
        self.inner.send(value).await.map_err(|error| error.0)
    }

    /// Wait until every receiver has been dropped.
    pub async fn closed(&self) {
        self.inner.closed().await;
    }
}

/// Receiving side of a bounded channel.
pub struct Receiver<T> {
    #[cfg(not(target_arch = "wasm32"))]
    inner: tokio::sync::mpsc::Receiver<T>,
    #[cfg(target_arch = "wasm32")]
    inner: async_channel::Receiver<T>,
}

impl<T> Receiver<T> {
    /// Receive the next value, or `None` once every sender is closed.
    pub async fn recv(&mut self) -> Option<T> {
        #[cfg(not(target_arch = "wasm32"))]
        return self.inner.recv().await;

        #[cfg(target_arch = "wasm32")]
        self.inner.recv().await.ok()
    }
}

pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    #[cfg(not(target_arch = "wasm32"))]
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    #[cfg(target_arch = "wasm32")]
    let (sender, receiver) = async_channel::bounded(capacity);

    (Sender { inner: sender }, Receiver { inner: receiver })
}

pub async fn recv_until_closed<T, U>(receiver: &mut Receiver<T>, sender: &Sender<U>) -> Option<T> {
    #[cfg(not(target_arch = "wasm32"))]
    return tokio::select! {
        _ = sender.inner.closed() => None,
        value = receiver.inner.recv() => value,
    };

    #[cfg(target_arch = "wasm32")]
    {
        use futures::future::{select, Either};

        let receive = Box::pin(receiver.inner.recv());
        let closed = Box::pin(sender.inner.closed());
        match select(receive, closed).await {
            Either::Left((Ok(value), _)) => Some(value),
            Either::Left((Err(_), _)) | Either::Right(_) => None,
        }
    }
}
