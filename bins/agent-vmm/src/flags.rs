//! The flag syntax of the appliance and MCP commands: `-name value`, `--name value`,
//! `-name=value`, and a boolean `-name`. Parsing stops at the first argument that is not a flag.

use std::collections::BTreeMap;
use std::time::Duration;

/// A flag takes a value, or it is a boolean that is set by its presence.
#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Value,
    Boolean,
}

#[derive(Debug, Default)]
pub struct Flags {
    values: BTreeMap<String, Vec<String>>,
    /// The arguments after the flags.
    pub rest: Vec<String>,
}

impl Flags {
    pub fn parse(arguments: &[String], accepted: &[(&str, Kind)]) -> Result<Self, String> {
        let mut flags = Self::default();
        let mut index = 0;
        while let Some(argument) = arguments.get(index) {
            index += 1;
            let Some(body) = argument.strip_prefix("--").or_else(|| argument.strip_prefix('-')).filter(|body| !body.is_empty()) else {
                // `--` ends the flags and is not an argument itself.
                flags.rest = arguments[if argument == "--" { index } else { index - 1 }..].to_vec();
                break;
            };
            if body.starts_with('-') || body.starts_with('=') {
                return Err(format!("bad flag syntax: {argument}"));
            }
            let (name, inline) = body.split_once('=').map_or((body, None), |(name, value)| (name, Some(value)));
            let Some((_, kind)) = accepted.iter().find(|(accepted, _)| *accepted == name) else {
                return Err(format!("flag provided but not defined: -{name}"));
            };
            let value = match (inline, kind) {
                (Some(value), Kind::Boolean) if !matches!(value, "true" | "false" | "1" | "0" | "t" | "f" | "T" | "F" | "TRUE" | "FALSE" | "True" | "False") => {
                    return Err(format!("invalid boolean value {value:?} for -{name}"));
                }
                (Some(value), _) => value.to_string(),
                (None, Kind::Boolean) => "true".to_string(),
                (None, Kind::Value) => {
                    index += 1;
                    arguments.get(index - 1).cloned().ok_or_else(|| format!("flag needs an argument: -{name}"))?
                }
            };
            flags.values.entry(name.to_string()).or_default().push(value);
        }
        Ok(flags)
    }

    /// The last value of a flag.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).and_then(|values| values.last()).map(String::as_str)
    }

    pub fn text(&self, name: &str) -> String {
        self.get(name).unwrap_or_default().to_string()
    }

    pub fn all(&self, name: &str) -> &[String] {
        self.values.get(name).map_or(&[], Vec::as_slice)
    }

    pub fn enabled(&self, name: &str) -> bool {
        matches!(self.get(name), Some("true" | "1" | "t" | "T" | "TRUE" | "True"))
    }

    pub fn number(&self, name: &str) -> Result<Option<u64>, String> {
        self.get(name).map(|value| value.parse::<u64>().map_err(|_| format!("invalid value {value:?} for flag -{name}: parse error"))).transpose()
    }

    pub fn duration(&self, name: &str, default: Duration) -> Result<Duration, String> {
        self.get(name).map_or(Ok(default), |value| parse_duration(value).ok_or_else(|| format!("invalid value {value:?} for flag -{name}: parse error")))
    }
}

/// Parses a duration such as `90s`, `10m`, `1h30m` or `1.5s`. A negative duration is refused.
pub fn parse_duration(text: &str) -> Option<Duration> {
    if text == "0" {
        return Some(Duration::ZERO);
    }
    let mut rest = text.strip_prefix('+').unwrap_or(text);
    let mut total = 0f64;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest.find(|character: char| !character.is_ascii_digit() && character != '.').unwrap_or(rest.len());
        let value: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit = rest.find(|character: char| character.is_ascii_digit() || character == '.').unwrap_or(rest.len());
        let scale = match &rest[..unit] {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        total += value * scale;
        rest = &rest[unit..];
    }
    Duration::try_from_secs_f64(total).ok()
}

/// Prints a duration as Go does: `10m0s`, `1h0m0s`, `1.5s`, `250ms`.
pub fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    let fraction = |value: u128, unit: u128| {
        let text = format!("{}.{:0width$}", value / unit, value % unit, width = unit.ilog10() as usize);
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    };
    match nanos {
        0 => "0s".into(),
        1..1_000 => format!("{nanos}ns"),
        1_000..1_000_000 => format!("{}µs", fraction(nanos, 1_000)),
        1_000_000..1_000_000_000 => format!("{}ms", fraction(nanos, 1_000_000)),
        _ => {
            let seconds = fraction(nanos % 60_000_000_000, 1_000_000_000);
            let minutes = nanos / 60_000_000_000;
            match (minutes / 60, minutes % 60) {
                (0, 0) => format!("{seconds}s"),
                (0, minutes) => format!("{minutes}m{seconds}s"),
                (hours, minutes) => format!("{hours}h{minutes}m{seconds}s"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn flags_accept_one_or_two_dashes_and_inline_values() {
        let accepted = [("installation", Kind::Value), ("execute", Kind::Boolean), ("confirm-resource", Kind::Value), ("expected-revision", Kind::Value), ("timeout", Kind::Value)];
        let flags = Flags::parse(&arguments(&["--installation", "mcp-1", "-execute", "--confirm-resource=a", "-confirm-resource", "b", "--expected-revision", "7", "--timeout", "90s", "rest", "--ignored"]), &accepted).unwrap();
        assert_eq!((flags.text("installation").as_str(), flags.enabled("execute"), flags.all("confirm-resource")), ("mcp-1", true, arguments(&["a", "b"]).as_slice()));
        assert_eq!((flags.number("expected-revision"), flags.duration("timeout", Duration::ZERO)), (Ok(Some(7)), Ok(Duration::from_secs(90))));
        assert_eq!((flags.get("ignored"), flags.number("absent"), flags.duration("absent", Duration::from_secs(600))), (None, Ok(None), Ok(Duration::from_secs(600))));
        assert_eq!(flags.rest, arguments(&["rest", "--ignored"]));
        assert_eq!(Flags::parse(&arguments(&["--execute", "--", "-x"]), &accepted).unwrap().rest, arguments(&["-x"]));
        assert!(!Flags::parse(&arguments(&["--execute=false"]), &accepted).unwrap().enabled("execute"));
        for refused in [&["--unknown"][..], &["--installation"], &["---installation", "a"], &["--execute=maybe"]] {
            assert!(Flags::parse(&arguments(refused), &accepted).is_err(), "{refused:?}");
        }
        assert!(Flags::parse(&arguments(&["--expected-revision", "-1"]), &accepted).unwrap().number("expected-revision").is_err());
    }

    #[test]
    fn durations_use_the_go_syntax() {
        for (text, seconds) in [("10m", 600.0), ("90s", 90.0), ("1h30m", 5400.0), ("1.5s", 1.5), ("250ms", 0.25), ("0", 0.0)] {
            assert_eq!(parse_duration(text), Some(Duration::from_secs_f64(seconds)), "{text}");
        }
        for text in ["", "10", "-5s", "5x", "s"] {
            assert_eq!(parse_duration(text), None, "{text}");
        }
        for (seconds, text) in [(600.0, "10m0s"), (3600.0, "1h0m0s"), (5430.0, "1h30m30s"), (1.5, "1.5s"), (0.25, "250ms"), (45.0, "45s"), (0.0, "0s")] {
            assert_eq!(format_duration(Duration::from_secs_f64(seconds)), text);
        }
    }
}
