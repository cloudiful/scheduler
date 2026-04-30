use crate::error::ExecutionGuardError;
use crate::observer::{NoopObserver, SchedulerEvent, SchedulerObserver};
use crate::{ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardRenewal, ExecutionSlot};
use std::future::Future;
use std::sync::Arc;
use tokio::time::{Instant, interval_at};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardedRunResult<T> {
    Completed(T),
    Contended,
}

#[derive(Clone)]
pub struct GuardedSession<G> {
    guard: Arc<G>,
    observer: Arc<dyn SchedulerObserver>,
    lease: crate::ExecutionLease,
    catch_up: bool,
    trigger_count: u32,
}

#[derive(Clone)]
pub struct GuardedRunner<G> {
    guard: Arc<G>,
    observer: Arc<dyn SchedulerObserver>,
}

impl<G> GuardedRunner<G>
where
    G: ExecutionGuard + Send + Sync + 'static,
{
    pub fn new(guard: G) -> Self {
        Self::with_observer(guard, NoopObserver)
    }

    pub fn with_observer<O>(guard: G, observer: O) -> Self
    where
        O: SchedulerObserver,
    {
        Self {
            guard: Arc::new(guard),
            observer: Arc::new(observer),
        }
    }

    pub async fn run<F, Fut, T>(
        &self,
        slot: ExecutionSlot,
        run: F,
    ) -> Result<GuardedRunResult<T>, ExecutionGuardError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        self.run_with_metadata(slot, false, 0, run).await
    }

    pub async fn run_with_metadata<F, Fut, T>(
        &self,
        slot: ExecutionSlot,
        catch_up: bool,
        trigger_count: u32,
        run: F,
    ) -> Result<GuardedRunResult<T>, ExecutionGuardError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let Some(session) = self
            .acquire_with_metadata(slot, catch_up, trigger_count)
            .await?
        else {
            return Ok(GuardedRunResult::Contended);
        };

        Ok(GuardedRunResult::Completed(session.run(run).await))
    }

    pub async fn acquire(
        &self,
        slot: ExecutionSlot,
    ) -> Result<Option<GuardedSession<G>>, ExecutionGuardError> {
        self.acquire_with_metadata(slot, false, 0).await
    }

    async fn acquire_with_metadata(
        &self,
        slot: ExecutionSlot,
        catch_up: bool,
        trigger_count: u32,
    ) -> Result<Option<GuardedSession<G>>, ExecutionGuardError> {
        let acquired = self.guard.acquire(slot.clone()).await.map_err(|error| {
            let kind = G::classify_error(&error);
            ExecutionGuardError::new(error, kind)
        })?;

        let ExecutionGuardAcquire::Acquired(lease) = acquired else {
            self.observer
                .on_event(&SchedulerEvent::ExecutionGuardContended {
                    job_id: slot.job_id,
                    resource_id: slot.resource_id,
                    scope: slot.scope,
                    scheduled_at: slot.scheduled_at,
                    catch_up,
                    trigger_count,
                });
            return Ok(None);
        };

        self.observer
            .on_event(&SchedulerEvent::ExecutionGuardAcquired {
                job_id: lease.job_id.clone(),
                resource_id: lease.resource_id.clone(),
                scope: lease.scope,
                lease_key: lease.lease_key.clone(),
                scheduled_at: lease.scheduled_at,
                catch_up,
                trigger_count,
            });

        Ok(Some(GuardedSession {
            guard: self.guard.clone(),
            observer: self.observer.clone(),
            lease,
            catch_up,
            trigger_count,
        }))
    }
}

impl<G> GuardedSession<G>
where
    G: ExecutionGuard + Send + Sync + 'static,
{
    pub async fn run<F, Fut, T>(self, run: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let mut renewal_count = 0u32;
        let mut failed_renewal_count = 0u32;
        let mut lost_reported = false;
        let task = run();
        tokio::pin!(task);
        let mut renewal = self
            .guard
            .renew_interval(&self.lease)
            .map(|duration| interval_at(Instant::now() + duration, duration));

        let output = loop {
            if let Some(ticker) = renewal.as_mut() {
                let mut disable_renewal = false;
                let outcome = tokio::select! {
                    result = &mut task => Some(result),
                    _ = ticker.tick() => {
                        match self.guard.renew(&self.lease).await {
                            Ok(ExecutionGuardRenewal::Renewed) => {
                                renewal_count += 1;
                                self.observer.on_event(&SchedulerEvent::ExecutionGuardRenewed {
                                    job_id: self.lease.job_id.clone(),
                                    resource_id: self.lease.resource_id.clone(),
                                    scope: self.lease.scope,
                                    lease_key: self.lease.lease_key.clone(),
                                    scheduled_at: self.lease.scheduled_at,
                                    catch_up: self.catch_up,
                                    trigger_count: self.trigger_count,
                                    renewal_count,
                                });
                            }
                            Ok(ExecutionGuardRenewal::Lost) => {
                                if !lost_reported {
                                    self.observer.on_event(&SchedulerEvent::ExecutionGuardLost {
                                        job_id: self.lease.job_id.clone(),
                                        resource_id: self.lease.resource_id.clone(),
                                        scope: self.lease.scope,
                                        lease_key: self.lease.lease_key.clone(),
                                        scheduled_at: self.lease.scheduled_at,
                                        catch_up: self.catch_up,
                                        trigger_count: self.trigger_count,
                                        renewal_count,
                                        failed_renewal_count,
                                    });
                                    lost_reported = true;
                                }
                                disable_renewal = true;
                            }
                            Err(error) => {
                                failed_renewal_count += 1;
                                self.observer.on_event(&SchedulerEvent::ExecutionGuardRenewFailed {
                                    job_id: self.lease.job_id.clone(),
                                    resource_id: self.lease.resource_id.clone(),
                                    scope: self.lease.scope,
                                    lease_key: self.lease.lease_key.clone(),
                                    scheduled_at: self.lease.scheduled_at,
                                    catch_up: self.catch_up,
                                    trigger_count: self.trigger_count,
                                    renewal_count,
                                    failed_renewal_count,
                                    error: error.to_string(),
                                });
                                if !lost_reported {
                                    self.observer.on_event(&SchedulerEvent::ExecutionGuardLost {
                                        job_id: self.lease.job_id.clone(),
                                        resource_id: self.lease.resource_id.clone(),
                                        scope: self.lease.scope,
                                        lease_key: self.lease.lease_key.clone(),
                                        scheduled_at: self.lease.scheduled_at,
                                        catch_up: self.catch_up,
                                        trigger_count: self.trigger_count,
                                        renewal_count,
                                        failed_renewal_count,
                                    });
                                    lost_reported = true;
                                }
                                disable_renewal = true;
                            }
                        }
                        None
                    }
                };

                if disable_renewal {
                    renewal = None;
                }

                if let Some(result) = outcome {
                    break result;
                }
            } else {
                break task.await;
            }
        };

        if let Err(error) = self.guard.release(&self.lease).await {
            self.observer
                .on_event(&SchedulerEvent::ExecutionGuardReleaseFailed {
                    job_id: self.lease.job_id.clone(),
                    resource_id: self.lease.resource_id.clone(),
                    scope: self.lease.scope,
                    lease_key: self.lease.lease_key.clone(),
                    scheduled_at: self.lease.scheduled_at,
                    catch_up: self.catch_up,
                    trigger_count: self.trigger_count,
                    error: error.to_string(),
                });
        } else {
            self.observer
                .on_event(&SchedulerEvent::ExecutionGuardReleased {
                    job_id: self.lease.job_id.clone(),
                    resource_id: self.lease.resource_id.clone(),
                    scope: self.lease.scope,
                    lease_key: self.lease.lease_key.clone(),
                    scheduled_at: self.lease.scheduled_at,
                    catch_up: self.catch_up,
                    trigger_count: self.trigger_count,
                });
        }

        output
    }
}
