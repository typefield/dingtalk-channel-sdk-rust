//! Per-chat serial + batch queue manager (Go `safety/chat_queue.go` port).
//!
//! Two modes per scope (usually conversationId):
//! - full mode: debounced batching + serial dispatch (messages / OnBatch)
//! - serial-only: no batching, strict FIFO execution (card actions etc.)

use crate::types::{BatchConfig, BatchedMessage, IncomingMessage, MediaBatchConfig, Mention};
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// Dispatch callback invoked with the merged batch.
pub type FlushHandler = Arc<dyn Fn(BatchedMessage) -> BoxFuture<'static, ()> + Send + Sync>;

type SerialTask = BoxFuture<'static, crate::error::Result<()>>;

enum Job {
    /// Pre-built dispatch/task future; executed strictly in order.
    Task(BoxFuture<'static, ()>),
    /// Serial task that reports its result back.
    Serial(SerialTask, oneshot::Sender<crate::error::Result<()>>),
    /// Ordering barrier.
    Barrier(oneshot::Sender<()>),
}

struct QueueState {
    buffer: Vec<Arc<IncomingMessage>>,
    buffer_chars: usize,
    pending_handler: Option<FlushHandler>,
}

pub struct ChatQueue {
    #[allow(dead_code)]
    scope: String,
    batch_cfg: Arc<BatchConfig>,
    media_batch: Arc<MediaBatchConfig>,
    serial_only: bool,
    state: Mutex<QueueState>,
    tx: mpsc::UnboundedSender<Job>,
    gen: std::sync::atomic::AtomicU64,
}

impl ChatQueue {
    fn new(
        scope: &str,
        batch_cfg: Arc<BatchConfig>,
        media_batch: Arc<MediaBatchConfig>,
        serial_only: bool,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let q = Arc::new(Self {
            scope: scope.to_string(),
            batch_cfg,
            media_batch,
            serial_only,
            state: Mutex::new(QueueState {
                buffer: Vec::new(),
                buffer_chars: 0,
                pending_handler: None,
            }),
            tx,
            gen: Default::default(),
        });
        tokio::spawn(worker_loop(rx));
        q
    }

    /// Push a message into the batch buffer (full mode only).
    pub async fn push(self: &Arc<Self>, msg: Arc<IncomingMessage>, handler: FlushHandler) {
        let flush_now = {
            let mut st = self.state.lock().unwrap();
            st.buffer.push(msg.clone());
            st.buffer_chars += msg.text.chars().count();
            if st.pending_handler.is_none() {
                st.pending_handler = Some(handler);
            }
            // Capacity reached → flush immediately.
            st.buffer.len() >= self.batch_cfg.max_messages
                || st.buffer_chars >= self.batch_cfg.max_chars
        };
        if flush_now || self.batch_cfg.delay_ms == 0 || self.serial_only {
            self.enqueue_flush();
            return;
        }
        // Debounce timer with generation guard (later pushes invalidate older timers).
        let delay_ms = {
            let st = self.state.lock().unwrap();
            let mut d = self.batch_cfg.delay_ms;
            if st.buffer_chars >= self.batch_cfg.long_threshold_chars {
                d = self.batch_cfg.long_delay_ms;
            }
            if self.media_batch.enabled && !msg.resources.is_empty() {
                d = self.media_batch.delay_ms;
            }
            d
        };
        let gen = self.gen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            if let Some(q) = weak.upgrade() {
                if q.gen.load(std::sync::atomic::Ordering::SeqCst) == gen {
                    q.enqueue_flush();
                }
            }
        });
    }

    /// Enqueue a flush of the current buffer as one ordered task.
    pub fn enqueue_flush(&self) {
        let (buffer, handler) = {
            let mut st = self.state.lock().unwrap();
            if st.buffer.is_empty() {
                return;
            }
            (std::mem::take(&mut st.buffer), st.pending_handler.take())
        };
        let Some(handler) = handler else { return };

        if self.serial_only {
            // Dispatch each message individually, in order, no merging.
            for m in buffer {
                let source_ids = vec![m.msg_id.clone()];
                let fut = handler(BatchedMessage {
                    message: m,
                    source_ids,
                });
                let _ = self.tx.send(Job::Task(fut));
            }
            return;
        }

        let source_ids: Vec<String> = buffer.iter().map(|m| m.msg_id.clone()).collect();
        let merged = merge_batch_messages(&buffer);
        let fut = handler(BatchedMessage {
            message: Arc::new(merged),
            source_ids,
        });
        let _ = self.tx.send(Job::Task(fut));
    }

    /// Execute a task strictly serialized against other work in this scope.
    pub async fn run_serial(&self, task: SerialTask) -> crate::error::Result<()> {
        // Flush any pending batch first to preserve ordering.
        self.enqueue_flush();
        let (rtx, rrx) = oneshot::channel();
        if self.tx.send(Job::Serial(task, rtx)).is_err() {
            return Err(crate::error::Error::channel("chat queue worker dropped"));
        }
        match rrx.await {
            Ok(res) => res,
            Err(_) => Err(crate::error::Error::channel("chat queue worker dropped")),
        }
    }

    /// Wait until all queued work is done.
    pub async fn flush_now(&self) {
        self.enqueue_flush();
        let (btx, brx) = oneshot::channel();
        if self.tx.send(Job::Barrier(btx)).is_ok() {
            let _ = brx.await;
        }
    }
}

async fn worker_loop(mut rx: mpsc::UnboundedReceiver<Job>) {
    while let Some(job) = rx.recv().await {
        match job {
            Job::Task(fut) => fut.await,
            Job::Serial(task, ack) => {
                let res = task.await;
                let _ = ack.send(res);
            }
            Job::Barrier(ack) => {
                let _ = ack.send(());
            }
        }
    }
}

/// Merge a message batch: last message is the base; texts join with blank lines;
/// resources and mentions dedup-merge; @all ORs across the batch.
pub(crate) fn merge_batch_messages(batch: &[Arc<IncomingMessage>]) -> IncomingMessage {
    if batch.len() == 1 {
        return IncomingMessage::clone(&batch[0]);
    }
    let last = batch.last().unwrap();

    let contents: Vec<&str> = batch
        .iter()
        .map(|m| m.text.as_str())
        .filter(|t| !t.is_empty())
        .collect();
    let content = contents.join("\n\n");

    let mut mention_all = false;
    let mut resources: Vec<crate::types::Resource> = Vec::new();
    let mut mentions: Vec<Mention> = Vec::new();
    let mut seen_resources = std::collections::HashSet::new();
    let mut seen_mentions = std::collections::HashSet::new();

    for m in batch {
        mention_all |= m.mention_all;
        for r in &m.resources {
            if seen_resources.insert(r.download_code.clone()) {
                resources.push(r.clone());
            }
        }
        for mention in &m.mentions {
            let key = if mention.user_id.is_empty() {
                &mention.name
            } else {
                &mention.user_id
            };
            if seen_mentions.insert(key.clone()) {
                mentions.push(mention.clone());
            }
        }
    }

    let mut merged = IncomingMessage::clone(&**last);
    merged.text = content;
    merged.mention_all = mention_all;
    merged.resources = resources;
    merged.mentions = mentions;
    merged.batched_sources = batch.to_vec();
    merged
}

/// Manager indexing queues per scope.
#[derive(Clone)]
pub struct ChatQueueManager {
    inner: Arc<Inner>,
}

struct Inner {
    batch_cfg: Arc<BatchConfig>,
    media_batch: Arc<MediaBatchConfig>,
    enabled: bool,
    queues: Mutex<HashMap<String, Arc<ChatQueue>>>,
}

impl ChatQueueManager {
    pub fn new(batch_cfg: BatchConfig, queue_enabled: bool, media_batch: MediaBatchConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                batch_cfg: Arc::new(batch_cfg),
                media_batch: Arc::new(media_batch),
                enabled: queue_enabled,
                queues: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    fn get_or_create(&self, scope: &str, serial_only: bool) -> Arc<ChatQueue> {
        {
            let qs = self.inner.queues.lock().unwrap();
            if let Some(q) = qs.get(scope) {
                return q.clone();
            }
        }
        let mut qs = self.inner.queues.lock().unwrap();
        qs.entry(scope.to_string())
            .or_insert_with(|| {
                ChatQueue::new(
                    scope,
                    self.inner.batch_cfg.clone(),
                    self.inner.media_batch.clone(),
                    serial_only,
                )
            })
            .clone()
    }

    /// Push a message to the scoped batch queue.
    pub async fn push(&self, scope: &str, msg: Arc<IncomingMessage>, handler: FlushHandler) {
        let q = self.get_or_create(scope, false);
        q.push(msg, handler).await;
    }

    /// Run a task serialized within the scope (serial-only queue).
    pub async fn run_serial(
        &self,
        scope: &str,
        task: BoxFuture<'static, crate::error::Result<()>>,
    ) -> crate::error::Result<()> {
        let q = self.get_or_create(scope, true);
        q.run_serial(task).await
    }

    /// Flush every queue concurrently.
    pub async fn flush_all(&self) {
        let queues: Vec<Arc<ChatQueue>> = self
            .inner
            .queues
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let joins: Vec<_> = queues
            .into_iter()
            .map(|q| tokio::spawn(async move { q.flush_now().await }))
            .collect();
        for j in joins {
            let _ = j.await;
        }
    }
}
