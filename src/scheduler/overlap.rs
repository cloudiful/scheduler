use crate::model::OverlapPolicy;
use crate::scheduler::trigger::PendingTrigger;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverlapAction {
    Spawn(PendingTrigger),
    QueueUpdated,
    Dropped,
}

pub(crate) fn take_queued_if_idle(
    active_count: usize,
    queued_trigger: &mut Option<PendingTrigger>,
) -> Option<PendingTrigger> {
    if active_count == 0 {
        queued_trigger.take()
    } else {
        None
    }
}

pub(crate) fn dispatch_trigger(
    policy: OverlapPolicy,
    active_count: usize,
    queued_trigger: &mut Option<PendingTrigger>,
    trigger: PendingTrigger,
) -> OverlapAction {
    match policy {
        OverlapPolicy::AllowParallel => OverlapAction::Spawn(trigger),
        OverlapPolicy::Forbid => {
            if active_count == 0 {
                OverlapAction::Spawn(trigger)
            } else {
                OverlapAction::Dropped
            }
        }
        OverlapPolicy::QueueOne => {
            if active_count == 0 {
                OverlapAction::Spawn(trigger)
            } else {
                if queued_trigger.is_none() {
                    *queued_trigger = Some(trigger);
                }
                OverlapAction::QueueUpdated
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OverlapAction, dispatch_trigger, take_queued_if_idle};
    use crate::{OverlapPolicy, TriggerSource};
    use crate::scheduler::trigger::PendingTrigger;
    use chrono::Utc;

    fn trigger() -> PendingTrigger {
        PendingTrigger {
            scheduled_at: Utc::now(),
            catch_up: false,
            trigger_count: 1,
            source: TriggerSource::Scheduled,
        }
    }

    #[test]
    fn queue_one_keeps_existing_queued_trigger() {
        let existing = trigger();
        let mut queued_trigger = Some(existing);

        let action = dispatch_trigger(OverlapPolicy::QueueOne, 1, &mut queued_trigger, trigger());

        assert_eq!(action, OverlapAction::QueueUpdated);
        assert_eq!(queued_trigger, Some(existing));
    }

    #[test]
    fn forbid_drops_trigger_while_active() {
        let mut queued_trigger = None;

        let action = dispatch_trigger(OverlapPolicy::Forbid, 1, &mut queued_trigger, trigger());

        assert_eq!(action, OverlapAction::Dropped);
        assert!(queued_trigger.is_none());
    }

    #[test]
    fn queued_trigger_is_taken_only_when_idle() {
        let queued = trigger();
        let mut queued_trigger = Some(queued);

        assert_eq!(take_queued_if_idle(1, &mut queued_trigger), None);
        assert_eq!(take_queued_if_idle(0, &mut queued_trigger), Some(queued));
        assert!(queued_trigger.is_none());
    }
}
