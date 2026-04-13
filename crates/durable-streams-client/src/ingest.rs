//! Shared JSON and JSONL input loading for library and CLI workflows.

use crate::Error;
use serde_json::Value;
use std::fs;
use std::path::Path;

/// Normalized source format detected during JSON input loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonInputFormat {
    /// One top-level JSON value.
    JsonValue,
    /// One top-level JSON array expanded into logical items.
    JsonArray,
    /// Line-delimited JSON with one value per non-empty line.
    JsonLines,
}

/// Fully normalized JSON input.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonInput {
    format: JsonInputFormat,
    values: Vec<Value>,
}

impl JsonInput {
    /// Return the detected input format.
    #[must_use]
    pub fn format(&self) -> JsonInputFormat {
        self.format
    }

    /// Borrow the normalized JSON values.
    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consume the input and return the normalized values.
    #[must_use]
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

/// Load JSON values from a file path.
///
/// The loader accepts:
///
/// - one top-level JSON value
/// - one top-level JSON array, expanded into logical items
/// - line-delimited JSON with one value per non-empty line
pub fn load_json_input(path: impl AsRef<Path>) -> Result<JsonInput, Error> {
    let bytes = fs::read(path)?;
    parse_json_input(&bytes)
}

/// Parse JSON values from an in-memory buffer.
///
/// Empty or whitespace-only input is treated as an empty JSONL stream.
pub fn parse_json_input(bytes: &[u8]) -> Result<JsonInput, Error> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(JsonInput {
            format: JsonInputFormat::JsonLines,
            values: Vec::new(),
        });
    }

    match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::Array(values)) => Ok(JsonInput {
            format: JsonInputFormat::JsonArray,
            values,
        }),
        Ok(value) => Ok(JsonInput {
            format: JsonInputFormat::JsonValue,
            values: vec![value],
        }),
        Err(document_error) => parse_json_lines(bytes).map_err(|line_error| {
            Error::parse(format!(
                "failed to parse JSON input as a single document ({document_error}) or JSONL ({line_error})"
            ))
        }),
    }
}

fn parse_json_lines(bytes: &[u8]) -> Result<JsonInput, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| format!("input contained invalid UTF-8: {error}"))?;
    let mut values = Vec::new();

    for (line_number, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str::<Value>(line)
            .map_err(|error| format!("invalid JSON on line {}: {error}", line_number + 1))?;
        values.push(value);
    }

    Ok(JsonInput {
        format: JsonInputFormat::JsonLines,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::{JsonInputFormat, parse_json_input};
    use serde_json::json;

    #[test]
    fn accepts_single_json_value() {
        let input = parse_json_input(br#"{"kind":"one"}"#).expect("single value parses");
        assert_eq!(input.format(), JsonInputFormat::JsonValue);
        assert_eq!(input.values(), &[json!({"kind":"one"})]);
    }

    #[test]
    fn accepts_json_array() {
        let input = parse_json_input(br#"[{"id":1},{"id":2}]"#).expect("array parses");
        assert_eq!(input.format(), JsonInputFormat::JsonArray);
        assert_eq!(input.values(), &[json!({"id": 1}), json!({"id": 2})]);
    }

    #[test]
    fn accepts_json_lines() {
        let input = parse_json_input(
            br#"{"id":1}
{"id":2}
"#,
        )
        .expect("json lines parse");
        assert_eq!(input.format(), JsonInputFormat::JsonLines);
        assert_eq!(input.values(), &[json!({"id": 1}), json!({"id": 2})]);
    }

    #[test]
    fn returns_clear_parse_errors() {
        let error = parse_json_input(
            br#"{"id":1}
not-json
"#,
        )
        .expect_err("invalid jsonl should fail");
        assert!(
            error.to_string().contains("line 2"),
            "unexpected error: {error}"
        );
    }
}
