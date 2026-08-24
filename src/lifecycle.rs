//! Connection lifecycle hooks (Go `lifecycle.go` port).

use std::sync::RwLock;

type Hook = std::sync::Arc<dyn Fn() + Send + Sync>;
type ErrorHook = std::sync::Arc<dyn Fn(&crate::error::Error) + Send + Sync>;

#[derive(Default)]
pub struct LifecycleHooks {
    inner: RwLock<HooksInner>,
}

#[derive(Default)]
struct HooksInner {
    on_ready: Vec<Hook>,
    on_error: Vec<ErrorHook>,
    on_reconnecting: Vec<Hook>,
    on_reconnected: Vec<Hook>,
    on_disconnected: Vec<Hook>,
}

impl LifecycleHooks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub fn on_ready(&self, f: impl Fn() + Send + Sync + 'static) {
        self.inner
            .write()
            .unwrap()
            .on_ready
            .push(std::sync::Arc::new(f));
    }
    pub fn on_error(&self, f: impl Fn(&crate::error::Error) + Send + Sync + 'static) {
        self.inner
            .write()
            .unwrap()
            .on_error
            .push(std::sync::Arc::new(f));
    }
    pub fn on_reconnecting(&self, f: impl Fn() + Send + Sync + 'static) {
        self.inner
            .write()
            .unwrap()
            .on_reconnecting
            .push(std::sync::Arc::new(f));
    }
    pub fn on_reconnected(&self, f: impl Fn() + Send + Sync + 'static) {
        self.inner
            .write()
            .unwrap()
            .on_reconnected
            .push(std::sync::Arc::new(f));
    }
    pub fn on_disconnected(&self, f: impl Fn() + Send + Sync + 'static) {
        self.inner
            .write()
            .unwrap()
            .on_disconnected
            .push(std::sync::Arc::new(f));
    }

    pub(crate) fn fire_ready(&self) {
        let g = self.inner.read().unwrap();
        for f in &g.on_ready {
            f();
        }
    }
    pub(crate) fn fire_error(&self, err: &crate::error::Error) {
        let g = self.inner.read().unwrap();
        for f in &g.on_error {
            f(err);
        }
    }
    pub(crate) fn fire_reconnecting(&self) {
        let g = self.inner.read().unwrap();
        for f in &g.on_reconnecting {
            f();
        }
    }
    pub(crate) fn fire_reconnected(&self) {
        let g = self.inner.read().unwrap();
        for f in &g.on_reconnected {
            f();
        }
    }
    pub(crate) fn fire_disconnected(&self) {
        let g = self.inner.read().unwrap();
        for f in &g.on_disconnected {
            f();
        }
    }
}
