use chrono::{TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::sync::atomic::AtomicUsize;

pub fn shanghai_after(milliseconds: i64) -> chrono::DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&Shanghai) + TimeDelta::milliseconds(milliseconds)
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct RefreshDeps {
    pub label: &'static str,
    pub seen: AtomicUsize,
}
