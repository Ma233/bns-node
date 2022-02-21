use crate::types::channel::Channel;
use crate::types::channel::Events;
use anyhow::Result;
use async_channel as ac;
use async_channel::Receiver;
use async_channel::Sender;
use async_trait::async_trait;
use std::sync::Arc;

pub struct AcChannel {
    sender: Arc<Sender<Events>>,
    receiver: Arc<Receiver<Events>>,
}

#[async_trait]
impl Channel for AcChannel {
    type Sender = Arc<Sender<Events>>;
    type Receiver = Arc<Receiver<Events>>;

    fn new(buffer: usize) -> Self {
        let (tx, rx) = ac::bounded(buffer);
        Self {
            sender: Arc::new(tx),
            receiver: Arc::new(rx),
        }
    }

    fn sender(&self) -> Self::Sender {
        Arc::clone(&self.sender)
    }

    fn receiver(&self) -> Self::Receiver {
        Arc::clone(&self.receiver)
    }

    async fn send(&self, e: Events) -> Result<()> {
        Ok(self.sender.send(e).await?)
    }

    async fn recv(&self) -> Result<Events> {
        self.receiver().recv().await.map_err(|e| anyhow::anyhow!(e))
    }
}
