use chrono::{DateTime, Local, NaiveDate, NaiveTime, Timelike};
use std::time::Duration;
use tokio::time;

#[derive(Default)]
pub struct Scheduler<R> {
    current_id: i32,
    pub plan: Plan,
    history: History<R>,
}

pub struct Plan {
    pub interval: Option<Duration>,
    pub date_time: Option<DateTime<Local>>,
    pub date: Option<NaiveDate>,
    pub time: Option<NaiveTime>,
    pub count: Option<usize>,
}

impl Default for Plan {
    fn default() -> Self {
        Plan {
            interval: Some(Duration::from_millis(100)),
            date_time: None,
            date: None,
            time: None,
            count: Some(1),
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
        let mut interval;
        match self.plan.interval {
            None => {
                interval = time::interval(Duration::from_millis(1000));
            }
            Some(duration) => {
                interval = time::interval(duration);
            }
        }

        loop {
            interval.tick().await;

            if self.skip() {
                continue;
            }

            self.execute(&func, args.clone()).await;

            if self.plan.count == Some(self.history.runtime.len()) {
                return self.history.results.clone();
            }
        }
    }

    fn skip(&self) -> bool {
        if self.plan.date_time.is_none() {
            return false;
        }

        let now = Local::now();
        now.hour() == self.plan.date_time.unwrap().hour()
            && now.minute() == self.plan.date_time.unwrap().minute()
            && now.second() == self.plan.date_time.unwrap().second()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    async fn add(num: i32) -> String {
        format!("result, {}!", num + 1)
    }

    #[tokio::test]
    async fn datetime_once() {
        let mut scheduler = Scheduler::default();
        scheduler.plan.date_time = Some(Local::now());
        let result = scheduler.run(add, 1).await;
        assert_eq!(result, vec!["result, 2!"]);
    }

    #[tokio::test]
    async fn interval_twice() {
        let mut scheduler = Scheduler::default();
        scheduler.plan.interval = Some(Duration::from_secs(3));
        scheduler.plan.count = Some(3);
        let result = scheduler.run(add, 1).await;
        assert_eq!(result, vec!["result, 2!", "result, 2!", "result, 2!"]);
    }
}
