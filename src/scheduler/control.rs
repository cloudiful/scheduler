use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlSignal {
    Running,
    Cancel,
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SchedulerHandle {
    control: watch::Sender<ControlSignal>,
}

impl SchedulerHandle {
    pub(crate) fn new(control: watch::Sender<ControlSignal>) -> Self {
        Self { control }
    }

    pub fn cancel(&self) {
        let _ = self.control.send(ControlSignal::Cancel);
    }

    pub fn shutdown(&self) {
        let _ = self.control.send(ControlSignal::Shutdown);
    }
}
