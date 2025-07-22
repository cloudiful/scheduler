use chrono::{DateTime, Local};
use std::time::Duration;

#[derive(Default)]
pub struct Scheduler<R> {
    current_id: i32,
    pub plan: Plan,
    history: History<R>,
}

pub struct Plan {
    pub interval: Option<Duration>,
    pub date_times: Option<Vec<DateTime<Local>>>,
    pub count: Option<i32>,
}

impl Default for Plan {
    fn default() -> Self {
        Plan {
            interval: None,
            date_times: None,
            count: None,
        }
    }
}

#[derive(Default)]
struct History<R> {
    runtime: Vec<DateTime<Local>>,
    results: Vec<R>,
}

impl<R> Scheduler<R>
where
    R: Clone,
{
    pub(crate) async fn execute<F, A>(&mut self, func: F, args: A) -> R
    where
        F: AsyncFnOnce(A) -> R,
    {
        self.current_id += 1;
        log::info!("Executing task {}", self.current_id);

        let future = func(args);
        let result = future.await;

        self.history.runtime.push(Local::now());
        self.history.results.push(result.clone());

        log::info!("Finished task {}", self.current_id);

        result
    }

    pub async fn run<F, A>(&mut self, func: F, args: A) -> Vec<R>
    where
        F: AsyncFn(A) -> R,
        A: Clone,
    {
        let mut duration;
        if let Some(value) = self.plan.interval {
            duration = value;
        } else {
            duration = Duration::from_secs(1);
        }

        let mut count = self.plan.count;
        if let Some(value) = &self.plan.date_times {
            count = Some(value.len() as i32);
        }

        loop {
            self.execute(&func, args.clone()).await;

            if let Some(calculated) = self.calculate_interval() {
                duration = calculated
            }

            if let Some(value) = count {
                if value <= self.current_id {
                    return self.history.results.clone();
                }
            }

            tokio::time::sleep(duration).await;
        }
    }

    fn calculate_interval(&mut self) -> Option<Duration> {
        let date_times;
        let date_time;

        if let Some(value) = &self.plan.date_times {
            date_times = value;
        } else {
            return None;
        }

        if let Some(value) = date_times.get((self.current_id - 1) as usize) {
            date_time = value;
        } else {
            return None;
        }

        let now = Local::now();

        let duration;
        if let Ok(value) = (date_time.clone() - now).to_std() {
            duration = value;
        } else {
            return None;
        }

        if let Some(value) = self.plan.interval {
            if value < duration {
                return None;
            }
        }

        Some(duration)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeDelta;
    use std::ops::Add;

    async fn add(num: i32) -> i32 {
        num + 1
    }

    async fn print_test(args: Option<String>) -> Option<String> {
        args
    }

    #[tokio::test]
    async fn duration_2() {
        let mut scheduler = Scheduler::default();
        scheduler.plan.interval = Some(Duration::from_secs(3));
        scheduler.plan.count = Some(2);
        let result = scheduler.run(print_test, None).await;
        assert_eq!(result, vec![None, None]);
    }

    #[tokio::test]
    async fn date_times() {
        let mut scheduler = Scheduler::default();
        scheduler.plan.date_times = Some(vec![
            Local::now().add(TimeDelta::seconds(1)),
            Local::now().add(TimeDelta::seconds(3)),
            Local::now().add(TimeDelta::seconds(5)),
        ]);
        let result = scheduler.run(add, 1).await;
        assert_eq!(result, vec![2, 2, 2]);
    }

    #[tokio::test]
    async fn interval_twice() {
        let mut scheduler = Scheduler::default();
        scheduler.plan.interval = Some(Duration::from_secs(2));
        scheduler.plan.count = Some(3);
        let result = scheduler.run(add, 1).await;
        assert_eq!(result, vec![2, 2, 2]);
    }
}
