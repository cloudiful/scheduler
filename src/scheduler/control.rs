use crate::error::SchedulerError;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchedulerMode {
    Running,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopSignal {
    Cancel,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandDisposition {
    Apply,
    ObserveOnly { changed: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ControlSignal {
    pub(crate) desired_mode: SchedulerMode,
    pub(crate) mode_command_seq: u64,
    pub(crate) poke_seq: u64,
    pub(crate) command_disposition: CommandDisposition,
    pub(crate) stop_signal: Option<StopSignal>,
}

impl ControlSignal {
    pub(crate) fn running() -> Self {
        Self {
            desired_mode: SchedulerMode::Running,
            mode_command_seq: 0,
            poke_seq: 0,
            command_disposition: CommandDisposition::Apply,
            stop_signal: None,
        }
    }
}

pub(crate) trait PauseController: Send + Sync + 'static {
    fn pause<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, SchedulerError>> + Send + 'a>>;

    fn resume<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, SchedulerError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ManualTriggerRequest {
    pub(crate) requested_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct SchedulerHandle {
    control: watch::Sender<ControlSignal>,
    manual_triggers: Arc<Mutex<HashMap<String, ManualTriggerRequest>>>,
    applied_modes: Arc<Mutex<HashMap<String, SchedulerMode>>>,
    pause_controller: Option<Arc<dyn PauseController>>,
    active_job_ids: Arc<Mutex<HashSet<String>>>,
}

impl SchedulerHandle {
    pub(crate) fn new(
        control: watch::Sender<ControlSignal>,
        manual_triggers: Arc<Mutex<HashMap<String, ManualTriggerRequest>>>,
        applied_modes: Arc<Mutex<HashMap<String, SchedulerMode>>>,
        pause_controller: Option<Arc<dyn PauseController>>,
        active_job_ids: Arc<Mutex<HashSet<String>>>,
    ) -> Self {
        Self {
            control,
            manual_triggers,
            applied_modes,
            pause_controller,
            active_job_ids,
        }
    }

    pub fn cancel(&self) {
        self.send_stop_signal(StopSignal::Cancel);
    }

    pub fn shutdown(&self) {
        self.send_stop_signal(StopSignal::Shutdown);
    }

    pub async fn pause(&self) -> Result<(), SchedulerError> {
        let active_job = self.wait_for_single_active_job_id().await?;
        if let Some(controller) = &self.pause_controller {
            match active_job.clone() {
                Some(job_id) => {
                    let changed = controller.pause(&job_id).await?;
                    self.send_mode_command(
                        SchedulerMode::Paused,
                        CommandDisposition::ObserveOnly { changed },
                    );
                }
                None => {
                    self.send_mode_command(SchedulerMode::Paused, CommandDisposition::Apply);
                }
            }
        } else {
            self.send_mode_command(SchedulerMode::Paused, CommandDisposition::Apply);
        }
        if let Some(job_id) = active_job {
            self.wait_for_mode(&job_id, SchedulerMode::Paused).await;
        }
        Ok(())
    }

    pub async fn resume(&self) -> Result<(), SchedulerError> {
        let active_job = self.wait_for_single_active_job_id().await?;
        if let Some(controller) = &self.pause_controller {
            match active_job.clone() {
                Some(job_id) => {
                    let changed = controller.resume(&job_id).await?;
                    self.send_mode_command(
                        SchedulerMode::Running,
                        CommandDisposition::ObserveOnly { changed },
                    );
                }
                None => {
                    self.send_mode_command(SchedulerMode::Running, CommandDisposition::Apply);
                }
            }
        } else {
            self.send_mode_command(SchedulerMode::Running, CommandDisposition::Apply);
        }
        if let Some(job_id) = active_job {
            self.wait_for_mode(&job_id, SchedulerMode::Running).await;
        }
        Ok(())
    }

    pub async fn trigger_now(&self) -> Result<(), SchedulerError> {
        let Some(job_id) = self.single_active_job_id()? else {
            return Err(SchedulerError::invalid_job_with_kind(
                crate::InvalidJobKind::Other,
                "trigger_now requires exactly one active job",
            ));
        };
        self.manual_triggers.lock().unwrap().insert(job_id, ManualTriggerRequest {
            requested_at: Utc::now(),
        });
        let mut next = *self.control.borrow();
        next.poke_seq = next.poke_seq.saturating_add(1);
        let _ = self.control.send(next);
        Ok(())
    }

    fn send_mode_command(
        &self,
        desired_mode: SchedulerMode,
        command_disposition: CommandDisposition,
    ) {
        let mut next = *self.control.borrow();
        next.desired_mode = desired_mode;
        next.command_disposition = command_disposition;
        next.mode_command_seq = next.mode_command_seq.saturating_add(1);
        let _ = self.control.send(next);
    }

    fn send_stop_signal(&self, stop_signal: StopSignal) {
        let mut next = *self.control.borrow();
        next.stop_signal = Some(stop_signal);
        let _ = self.control.send(next);
    }

    fn single_active_job_id(&self) -> Result<Option<String>, SchedulerError> {
        let active_job_ids = self.active_job_ids.lock().unwrap();
        match active_job_ids.len() {
            0 => Ok(None),
            1 => Ok(active_job_ids.iter().next().cloned()),
            _ => Err(SchedulerError::invalid_job_with_kind(
                crate::InvalidJobKind::Other,
                "pause/resume/trigger_now requires exactly one active job for coordinated schedulers",
            )),
        }
    }

    async fn wait_for_mode(&self, job_id: &str, expected_mode: SchedulerMode) {
        for _ in 0..100 {
            if self
                .applied_modes
                .lock()
                .unwrap()
                .get(job_id)
                .copied()
                == Some(expected_mode)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    async fn wait_for_single_active_job_id(&self) -> Result<Option<String>, SchedulerError> {
        for _ in 0..20 {
            match self.single_active_job_id()? {
                Some(job_id) => return Ok(Some(job_id)),
                None => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            }
        }
        self.single_active_job_id()
    }
}
