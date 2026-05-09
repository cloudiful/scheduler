use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub struct RefreshDeps {
    pub label: &'static str,
    pub seen: AtomicUsize,
}
