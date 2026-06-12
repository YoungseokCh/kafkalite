use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::watch;

use super::KafkaBroker;

#[derive(Debug, Default)]
pub(crate) struct FetchSignals {
    signals: Mutex<HashMap<(String, i32), watch::Sender<u64>>>,
}

impl FetchSignals {
    fn sender_for(&self, topic: &str, partition: i32) -> watch::Sender<u64> {
        let mut signals = self.signals.lock().expect("fetch signals mutex poisoned");
        signals
            .entry((topic.to_string(), partition))
            .or_insert_with(|| watch::channel(0).0)
            .clone()
    }

    pub(crate) fn subscribe(&self, topic: &str, partition: i32) -> watch::Receiver<u64> {
        self.sender_for(topic, partition).subscribe()
    }

    pub(crate) fn notify(&self, topic: &str, partition: i32) {
        let sender = self.sender_for(topic, partition);
        let next_generation = sender.borrow().wrapping_add(1);
        let _ = sender.send(next_generation);
    }

    #[cfg(test)]
    pub(crate) fn signal_count(&self) -> usize {
        self.signals
            .lock()
            .expect("fetch signals mutex poisoned")
            .len()
    }
}

impl KafkaBroker {
    pub fn subscribe_fetch_signal(&self, topic: &str, partition: i32) -> watch::Receiver<u64> {
        self.fetch_signals.subscribe(topic, partition)
    }

    pub fn notify_fetch_signal(&self, topic: &str, partition: i32) {
        self.fetch_signals.notify(topic, partition);
    }

    #[cfg(test)]
    pub(crate) fn fetch_signal_count(&self) -> usize {
        self.fetch_signals.signal_count()
    }
}
