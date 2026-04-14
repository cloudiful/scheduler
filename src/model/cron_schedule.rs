use crate::error::SchedulerError;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule as ParsedCronSchedule;
use std::fmt::{self, Debug, Display, Formatter};
use std::str::FromStr;

#[derive(Clone)]
pub struct CronSchedule {
    source: String,
    parsed: ParsedCronSchedule,
}

impl CronSchedule {
    pub fn parse(expression: &str) -> Result<Self, SchedulerError> {
        validate_field_count(expression)?;

        let parsed_expression = format!("0 {expression}");
        let parsed = ParsedCronSchedule::from_str(&parsed_expression).map_err(|error| {
            SchedulerError::invalid_cron(format!("invalid cron expression `{expression}`: {error}"))
        })?;

        Ok(Self {
            source: expression.to_string(),
            parsed,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    pub(crate) fn next_after(&self, after: DateTime<Utc>, timezone: Tz) -> Option<DateTime<Utc>> {
        let after = after.with_timezone(&timezone);
        self.parsed
            .after(&after)
            .next()
            .map(|value| value.with_timezone(&Utc))
    }
}

impl Debug for CronSchedule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CronSchedule").field(&self.source).finish()
    }
}

impl Display for CronSchedule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

impl PartialEq for CronSchedule {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for CronSchedule {}

impl FromStr for CronSchedule {
    type Err = SchedulerError;

    fn from_str(expression: &str) -> Result<Self, Self::Err> {
        Self::parse(expression)
    }
}

impl TryFrom<&str> for CronSchedule {
    type Error = SchedulerError;

    fn try_from(expression: &str) -> Result<Self, Self::Error> {
        Self::parse(expression)
    }
}

fn validate_field_count(expression: &str) -> Result<(), SchedulerError> {
    let field_count = expression.split_whitespace().count();
    if field_count == 5 {
        return Ok(());
    }

    Err(SchedulerError::invalid_cron(format!(
        "cron expression must have exactly 5 fields (minute hour day-of-month month day-of-week), got {field_count}: `{expression}`"
    )))
}

#[cfg(test)]
mod tests {
    use super::CronSchedule;
    use chrono::{TimeZone, Timelike, Utc};
    use chrono_tz::{Asia::Shanghai, UTC};

    #[test]
    fn parses_standard_five_field_expression() {
        let schedule = CronSchedule::parse("*/15 9-17 * * Mon-Fri").unwrap();

        assert_eq!(schedule.as_str(), "*/15 9-17 * * Mon-Fri");
    }

    #[test]
    fn rejects_non_five_field_expressions() {
        let error = CronSchedule::parse("0 */5 * * * *").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cron expression must have exactly 5 fields")
        );

        let error = CronSchedule::parse("@hourly").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cron expression must have exactly 5 fields")
        );
    }

    #[test]
    fn rejects_invalid_five_field_expression() {
        let error = CronSchedule::parse("bogus * * * *").unwrap_err();
        let message = error.to_string();

        assert!(message.contains("invalid cron expression `bogus * * * *`"));
    }

    #[test]
    fn next_after_uses_configured_timezone() {
        let schedule = CronSchedule::parse("0 9 * * *").unwrap();
        let start = Utc.with_ymd_and_hms(2026, 4, 3, 0, 30, 0).unwrap();

        let shanghai_next = schedule.next_after(start, Shanghai).unwrap();
        let utc_next = schedule.next_after(start, UTC).unwrap();

        assert_eq!(
            shanghai_next,
            Utc.with_ymd_and_hms(2026, 4, 3, 1, 0, 0).unwrap()
        );
        assert_eq!(utc_next, Utc.with_ymd_and_hms(2026, 4, 3, 9, 0, 0).unwrap());
    }

    #[test]
    fn next_after_advances_from_the_scheduled_time() {
        let schedule = CronSchedule::parse("* * * * *").unwrap();
        let scheduled_at = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 0).unwrap();

        let next = schedule.next_after(scheduled_at, UTC).unwrap();

        assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 3, 1, 3, 0).unwrap());
        assert_eq!(next.second(), 0);
    }
}
