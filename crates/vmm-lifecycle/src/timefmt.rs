//! RFC 3339 timestamps in UTC, with the fraction that Go `time.RFC3339Nano` prints.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn civil(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 { month_index + 3 } else { month_index - 9 } as u32;
    (year_of_era + era * 400 + i64::from(month <= 2), month, day)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_index = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let day_of_year = (153 * month_index + 2) / 5 + i64::from(day) - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

pub fn format(seconds: i64, nanos: u32) -> String {
    let (year, month, day) = civil(seconds.div_euclid(86_400));
    let second_of_day = seconds.rem_euclid(86_400);
    let mut text = format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}", second_of_day / 3_600, second_of_day % 3_600 / 60, second_of_day % 60);
    if nanos != 0 {
        text.push_str(format!(".{nanos:09}").trim_end_matches('0'));
    }
    text.push('Z');
    text
}

pub fn format_system(time: SystemTime) -> String {
    let elapsed = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    format(elapsed.as_secs() as i64, elapsed.subsec_nanos())
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fraction](Z|±HH:MM)` and returns the Unix time.
pub fn parse(text: &str) -> Option<SystemTime> {
    let bytes = text.as_bytes();
    let number = |range: std::ops::Range<usize>| -> Option<u32> {
        let part = text.get(range)?;
        part.bytes().all(|byte| byte.is_ascii_digit()).then(|| part.parse().ok()).flatten()
    };
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || !matches!(bytes[10], b'T' | b't') || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    let month_days = [31, if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=12).contains(&month) || day == 0 || day > month_days[month as usize - 1] || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut rest = &text[19..];
    let mut nanos = 0u32;
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        nanos = format!("{:0<9}", &fraction[..digits.min(9)]).parse().ok()?;
        rest = &fraction[digits..];
    }
    let offset = match rest {
        "Z" | "z" => 0,
        _ => {
            let sign = match rest.as_bytes().first()? {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if rest.len() != 6 || rest.as_bytes()[3] != b':' {
                return None;
            }
            let (hours, minutes) = (rest[1..3].parse::<i64>().ok()?, rest[4..6].parse::<i64>().ok()?);
            if hours > 23 || minutes > 59 {
                return None;
            }
            sign * (hours * 3_600 + minutes * 60)
        }
    };
    let seconds = days_from_civil(i64::from(year), month, day) * 86_400 + i64::from(hour * 3_600 + minute * 60 + second) - offset;
    u64::try_from(seconds).ok().map(|seconds| UNIX_EPOCH + Duration::new(seconds, nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_round_trip_with_the_go_fraction() {
        assert_eq!(format(1_795_000_000, 0), "2026-11-18T11:06:40Z");
        assert_eq!(format(1_795_000_000, 120_000_000), "2026-11-18T11:06:40.12Z");
        assert_eq!(format(951_782_400, 1), "2000-02-29T00:00:00.000000001Z");
        let parsed = parse("2026-11-18T19:06:40.12+08:00").unwrap();
        assert_eq!(format_system(parsed), "2026-11-18T11:06:40.12Z");
        for invalid in ["", "2026-11-18", "2026-13-18T11:06:40Z", "2026-02-30T11:06:40Z", "2026-11-18T11:06:40", "2026-11-18T11:06:40.Z", "2026-11-18 11:06:40Z"] {
            assert!(parse(invalid).is_none(), "{invalid}");
        }
    }
}
