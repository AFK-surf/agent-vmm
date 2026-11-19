//! The owner file: a flat JSON object of strings and unsigned integers.

use std::path::PathBuf;

/// The non-root account that owns the Host service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    pub user: String,
    pub group: String,
    pub uid: u32,
    pub gid: u32,
    pub home: PathBuf,
}

fn quote(value: &str) -> String {
    let mut quoted = String::from('"');
    for character in value.chars() {
        match character {
            '"' | '\\' => quoted.extend(['\\', character]),
            control if (control as u32) < 0x20 => quoted.push_str(&format!("\\u{:04x}", control as u32)),
            other => quoted.push(other),
        }
    }
    quoted + "\""
}

impl Owner {
    pub fn encode(&self) -> String {
        format!("{{\"user\":{},\"group\":{},\"uid\":{},\"gid\":{},\"home\":{}}}\n", quote(&self.user), quote(&self.group), self.uid, self.gid, quote(&self.home.to_string_lossy()))
    }

    pub fn decode(text: &str) -> Option<Self> {
        let mut parser = Parser(text.trim().chars().peekable());
        let (mut user, mut group, mut home, mut uid, mut gid) = (None, None, None, None, None);
        parser.expect('{')?;
        loop {
            let key = parser.string()?;
            parser.expect(':')?;
            match key.as_str() {
                "user" => user = Some(parser.string()?),
                "group" => group = Some(parser.string()?),
                "home" => home = Some(parser.string()?),
                "uid" => uid = Some(parser.number()?),
                "gid" => gid = Some(parser.number()?),
                _ => return None,
            }
            match parser.next()? {
                ',' => continue,
                '}' => break,
                _ => return None,
            }
        }
        parser.0.next().is_none().then_some(())?;
        Some(Self { user: user?, group: group?, uid: uid?, gid: gid?, home: PathBuf::from(home?) })
    }
}

struct Parser<'a>(std::iter::Peekable<std::str::Chars<'a>>);

impl Parser<'_> {
    fn next(&mut self) -> Option<char> {
        self.0.by_ref().find(|character| !character.is_ascii_whitespace())
    }

    fn expect(&mut self, wanted: char) -> Option<()> {
        (self.next()? == wanted).then_some(())
    }

    fn string(&mut self) -> Option<String> {
        self.expect('"')?;
        let mut value = String::new();
        loop {
            match self.0.next()? {
                '"' => return Some(value),
                '\\' => match self.0.next()? {
                    'u' => {
                        let digits: String = self.0.by_ref().take(4).collect();
                        value.push(char::from_u32(u32::from_str_radix(&digits, 16).ok().filter(|_| digits.len() == 4)?)?);
                    }
                    escaped @ ('"' | '\\' | '/') => value.push(escaped),
                    _ => return None,
                },
                control if (control as u32) < 0x20 => return None,
                other => value.push(other),
            }
        }
    }

    fn number(&mut self) -> Option<u32> {
        let mut digits = String::from(self.next()?);
        while let Some(digit) = self.0.next_if(char::is_ascii_digit) {
            digits.push(digit);
        }
        digits.parse().ok()
    }
}
