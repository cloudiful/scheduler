use chrono::{TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;

pub fn shanghai_after(milliseconds: i64) -> chrono::DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&Shanghai) + TimeDelta::milliseconds(milliseconds)
}
