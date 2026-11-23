//! How the console writes quantities: fixed-width, so columns of them align.

use prost_types::Timestamp;

const KIB: f64 = 1024.0;

/// A byte count in binary units with three significant digits: `1.24 GiB`.
pub fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut scaled = value as f64;
    let mut unit = 0;
    while scaled >= KIB && unit + 1 < UNITS.len() {
        scaled /= KIB;
        unit += 1;
    }
    if unit == 0 {
        return format!("{value} B");
    }
    let digits = if scaled >= 100.0 {
        0
    } else if scaled >= 10.0 {
        1
    } else {
        2
    };
    format!("{scaled:.digits$} {}", UNITS[unit])
}

/// An integer with thousands separators: `12,480`.
pub fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// CPU time: `1 h 38 min`, `21 min 24 s`, `3.0 s`.
pub fn cpu_time(nanos: u64) -> String {
    let seconds = nanos / 1_000_000_000;
    if seconds >= 3_600 {
        format!("{} h {:02} min", seconds / 3_600, (seconds / 60) % 60)
    } else if seconds >= 60 {
        format!("{} min {:02} s", seconds / 60, seconds % 60)
    } else {
        format!("{:.1} s", nanos as f64 / 1e9)
    }
}

/// A share of a whole: `37.5 %`.
pub fn percent(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "0.0 %".into();
    }
    format!("{:.1} %", part as f64 * 100.0 / whole as f64)
}

/// The UTC time of day of `timestamp`: `09:14:07`.
pub fn clock(timestamp: Option<&Timestamp>) -> String {
    match timestamp {
        Some(t) if t.seconds > 0 => {
            let day = t.seconds.rem_euclid(86_400);
            format!("{:02}:{:02}:{:02}", day / 3_600, (day / 60) % 60, day % 60)
        }
        _ => "never".into(),
    }
}

/// The UTC time of day of `timestamp` with milliseconds: `09:14:07.412`.
pub fn clock_millis(timestamp: Option<&Timestamp>) -> String {
    match timestamp {
        Some(t) if t.seconds > 0 => format!("{}.{:03}", clock(timestamp), t.nanos / 1_000_000),
        _ => String::new(),
    }
}

/// How long before `now` something happened: `14 s ago`, `22 min ago`.
pub fn age(timestamp: Option<&Timestamp>, now: Option<&Timestamp>) -> String {
    let (Some(then), Some(now)) = (timestamp, now) else {
        return "never".into();
    };
    if then.seconds <= 0 {
        return "never".into();
    }
    let seconds = (now.seconds - then.seconds).max(0);
    if seconds < 2 {
        "now".into()
    } else if seconds < 120 {
        format!("{seconds} s ago")
    } else if seconds < 7_200 {
        format!("{} min ago", seconds / 60)
    } else {
        format!("{} h ago", seconds / 3_600)
    }
}

/// Bytes as lowercase hex in groups of four: `04a1 9f3c`.
pub fn hex_groups(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 5 / 2);
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 && index % 2 == 0 {
            out.push(' ');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities_keep_three_significant_digits() {
        for (value, text) in [
            (0, "0 B"),
            (1_023, "1023 B"),
            (1_024, "1.00 KiB"),
            (1_536 << 20, "1.50 GiB"),
            (214 << 20, "214 MiB"),
            (96 << 30, "96.0 GiB"),
        ] {
            assert_eq!(bytes(value), text);
        }
        assert_eq!(count(1_204), "1,204");
        assert_eq!(count(999), "999");
        assert_eq!(percent(3, 8), "37.5 %");
        assert_eq!(cpu_time(5_912_000_000_000), "1 h 38 min");
        assert_eq!(cpu_time(1_284_000_000_000), "21 min 24 s");
    }

    #[test]
    fn times_read_as_utc() {
        let now = Timestamp {
            seconds: 1_795_338_847,
            nanos: 412_000_000,
        };
        assert_eq!(clock(Some(&now)), "09:14:07");
        assert_eq!(clock_millis(Some(&now)), "09:14:07.412");
        let then = Timestamp {
            seconds: now.seconds - 1_326,
            nanos: 0,
        };
        assert_eq!(age(Some(&then), Some(&now)), "22 min ago");
        assert_eq!(age(None, Some(&now)), "never");
        assert_eq!(hex_groups(&[4, 0xa1, 0x9f, 0x3c]), "04a1 9f3c");
    }
}
