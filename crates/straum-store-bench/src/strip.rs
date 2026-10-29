//! Leading timestamps of the Loghub logs, stripped as `tools/dedup-survey`
//! does, so repeated log lines repeat byte for byte.

#[derive(Clone, Copy)]
pub enum Format {
    OpenStack,
    Ssh,
    Bgl,
}

/// The line without its leading timestamp (as `tools/dedup-survey`).
pub fn strip_timestamp(format: Format, line: &[u8]) -> Option<Vec<u8>> {
    fn token(s: &[u8]) -> Option<(&[u8], &[u8])> {
        let i = s.iter().position(|&b| b == b' ')?;
        Some((&s[..i], &s[i + 1..]))
    }
    fn shape(s: &[u8], p: &[u8]) -> bool {
        s.len() == p.len()
            && s.iter().zip(p).all(|(&c, &p)| {
                if p == b'd' {
                    c.is_ascii_digit()
                } else {
                    c == p
                }
            })
    }
    match format {
        Format::OpenStack => {
            let (_, rest) = token(line)?;
            let (date, rest) = token(rest)?;
            let (time, rest) = token(rest)?;
            (shape(date, b"dddd-dd-dd") && shape(time, b"dd:dd:dd.ddd")).then(|| rest.to_vec())
        }
        Format::Ssh => {
            let month = line.get(..3)?;
            let ok = month[0].is_ascii_uppercase() && month[1..].iter().all(u8::is_ascii_lowercase);
            let mut rest = line.get(3..)?;
            let spaces = rest.iter().take_while(|&&b| b == b' ').count();
            if !ok || spaces == 0 {
                return None;
            }
            rest = &rest[spaces..];
            let (day, rest) = token(rest)?;
            let (time, rest) = token(rest)?;
            let day_ok = (1..=2).contains(&day.len()) && day.iter().all(u8::is_ascii_digit);
            (day_ok && shape(time, b"dd:dd:dd")).then(|| rest.to_vec())
        }
        Format::Bgl => {
            let (label, rest) = token(line)?;
            let (epoch, rest) = token(rest)?;
            let (date, rest) = token(rest)?;
            let (_node, rest) = token(rest)?;
            let (time, rest) = token(rest)?;
            let ok = !epoch.is_empty()
                && epoch.iter().all(u8::is_ascii_digit)
                && shape(date, b"dddd.dd.dd")
                && shape(time, b"dddd-dd-dd-dd.dd.dd.dddddd");
            ok.then(|| [label, b" ", rest].concat())
        }
    }
}
