use chrono::{TimeDelta, Utc};
use scheduler::JobState;

pub fn fixture_state(job_id: &str) -> JobState {
    JobState {
        job_id: job_id.to_string(),
        trigger_count: 3,
        last_run_at: Some(Utc::now()),
        last_success_at: Some(Utc::now() + TimeDelta::seconds(1)),
        next_run_at: Some(Utc::now() + TimeDelta::seconds(10)),
        last_error: Some("integration".to_string()),
    }
}
