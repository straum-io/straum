//! Expected values in scenarios: an expected JSON value matches an actual
//! one if every field it names matches (fields it omits are not checked),
//! arrays have the same length and match element by element, and scalars
//! are equal. Special expected values: `"*"` matches anything; `{"$le": n}`
//! and `{"$ge": n}` compare integers (numbers or decimal strings);
//! `{"$len": n}` checks an array's length only.

use straum_proto::json::Value;

fn int(v: &Value) -> Option<i128> {
    match v {
        Value::Num(n) => Some(i128::from(*n)),
        Value::Str(s) => s.parse().ok(),
        _ => None,
    }
}

/// `Ok(())` or the path of the first mismatch with both values.
pub fn matches(expected: &Value, actual: &Value, path: &str) -> Result<(), String> {
    let fail = || format!("{path}: expected {expected:?}, got {actual:?}");
    match expected {
        Value::Str(s) if s == "*" => Ok(()),
        Value::Obj(fields) if fields.len() == 1 && fields[0].0.starts_with('$') => {
            let (op, want) = (&fields[0].0, &fields[0].1);
            match op.as_str() {
                "$len" => match (actual.as_array(), want.as_i64()) {
                    (Some(a), Some(n)) if a.len() as i64 == n => Ok(()),
                    _ => Err(fail()),
                },
                "$le" | "$ge" => match (int(want), int(actual)) {
                    (Some(w), Some(a)) if (op == "$le" && a <= w) || (op == "$ge" && a >= w) => {
                        Ok(())
                    }
                    _ => Err(fail()),
                },
                _ => Err(format!("{path}: unknown matcher {op}")),
            }
        }
        Value::Obj(fields) => {
            for (k, v) in fields {
                let a = actual
                    .get(k)
                    .ok_or_else(|| format!("{path}.{k}: missing in {actual:?}"))?;
                matches(v, a, &format!("{path}.{k}"))?;
            }
            Ok(())
        }
        Value::Arr(items) => {
            let a = actual.as_array().ok_or_else(fail)?;
            if a.len() != items.len() {
                return Err(format!(
                    "{path}: expected {} elements, got {}: {actual:?}",
                    items.len(),
                    a.len()
                ));
            }
            for (i, (e, x)) in items.iter().zip(a).enumerate() {
                matches(e, x, &format!("{path}[{i}]"))?;
            }
            Ok(())
        }
        e if e == actual => Ok(()),
        _ => Err(fail()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use straum_proto::json::parse;

    #[test]
    fn subset_and_matchers() {
        let actual = parse(r#"{"a": "5", "b": [1, 2], "c": {"d": true, "e": null}}"#).unwrap();
        for ok in [
            r#"{}"#,
            r#"{"a": "5"}"#,
            r#"{"a": {"$le": 5}, "b": {"$len": 2}}"#,
            r#"{"c": {"d": true}, "b": [1, "*"]}"#,
            r#"{"a": {"$ge": "4"}}"#,
        ] {
            assert!(matches(&parse(ok).unwrap(), &actual, "$").is_ok(), "{ok}");
        }
        for bad in [
            r#"{"a": "6"}"#,
            r#"{"b": [1]}"#,
            r#"{"z": 1}"#,
            r#"{"a": {"$le": 4}}"#,
            r#"{"a": {"$eq": 5}}"#,
        ] {
            assert!(
                matches(&parse(bad).unwrap(), &actual, "$").is_err(),
                "{bad}"
            );
        }
    }
}
