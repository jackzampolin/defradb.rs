use std::ffi::c_char;

use serde_json::Value as JsonValue;

use crate::types::{c_str_to_string, FfiResult};

/// Check if a JSON string represents an array.
///
/// # Safety
///
/// `json_data` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn is_json_array(json_data: *const c_char) -> FfiResult {
    let json_str = match c_str_to_string(json_data) {
        Some(s) => s,
        None => return FfiResult::error("json_data is null"),
    };

    // Try to parse to detect type
    let parsed: JsonValue = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => return FfiResult::error(format!("invalid JSON: {}", e)),
    };

    FfiResult::success(parsed.is_array().to_string())
}

/// Parse a Go-style duration string into nanoseconds.
///
/// Supports Go's duration format: "300ms", "1.5h", "2h45m30s", etc.
/// Valid units: ns, us (or µs), ms, s, m, h
///
/// # Safety
///
/// `duration_str` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn parse_duration(duration_str: *const c_char) -> FfiResult {
    let input = match c_str_to_string(duration_str) {
        Some(s) => s,
        None => return FfiResult::error("duration_str is null"),
    };

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return FfiResult::success("0");
    }

    match parse_go_duration(trimmed) {
        Ok(nanos) => FfiResult::success(nanos.to_string()),
        Err(e) => FfiResult::error(e),
    }
}

/// Parse a Go-style duration string into nanoseconds.
///
/// Also accepts plain integers, which are treated as seconds for backwards compatibility.
fn parse_go_duration(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(0);
    }

    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };

    // Check if it's a plain integer (backwards compatibility: treat as seconds)
    if s.chars().all(|c| c.is_ascii_digit()) {
        let secs: i64 = s.parse().map_err(|_| format!("invalid number: {}", s))?;
        let nanos = secs * 1_000_000_000;
        return Ok(if negative { -nanos } else { nanos });
    }

    let mut total_nanos: i64 = 0;
    let mut remaining = s;

    while !remaining.is_empty() {
        // Find the end of the number part (including decimal point)
        let num_end = remaining
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(remaining.len());

        if num_end == 0 {
            return Err(format!("invalid duration: {}", s));
        }

        let num_str = &remaining[..num_end];
        remaining = &remaining[num_end..];

        // Find the unit
        let unit_end = remaining
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(remaining.len());

        if unit_end == 0 {
            return Err(format!("missing unit in duration: {}", s));
        }

        let unit = &remaining[..unit_end];
        remaining = &remaining[unit_end..];

        // Parse number (can be float)
        let num: f64 = num_str
            .parse()
            .map_err(|_| format!("invalid number in duration: {}", num_str))?;

        // Convert to nanoseconds based on unit
        let nanos_per_unit: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3600.0 * 1_000_000_000.0,
            _ => return Err(format!("unknown unit in duration: {}", unit)),
        };

        total_nanos += (num * nanos_per_unit) as i64;
    }

    if negative {
        total_nanos = -total_nanos;
    }

    Ok(total_nanos)
}

/// Parse a JSON string array into a vector of strings.
///
/// This function handles both JSON arrays (e.g., `["a", "b", "c"]`) and
/// comma-separated strings (e.g., `"a,b,c"`) for backwards compatibility.
///
/// # Safety
///
/// `input` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn parse_string_array(input: *const c_char) -> FfiResult {
    let input_str = match c_str_to_string(input) {
        Some(s) => s,
        None => return FfiResult::error("input is null"),
    };

    let trimmed = input_str.trim();

    // Empty input returns empty array
    if trimmed.is_empty() {
        return FfiResult::success("[]");
    }

    // Try to parse as JSON array first
    if trimmed.starts_with('[') {
        match serde_json::from_str::<Vec<String>>(trimmed) {
            Ok(arr) => {
                let json = serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string());
                return FfiResult::success(json);
            }
            Err(e) => {
                return FfiResult::error(format!("invalid JSON array: {}", e));
            }
        }
    }

    // Fall back to comma-separated parsing
    let parts: Vec<String> = trimmed
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let json = serde_json::to_string(&parts).unwrap_or_else(|_| "[]".to_string());
    FfiResult::success(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_json_array() {
        use std::ffi::CString;

        // Array
        let json = CString::new("[1, 2, 3]").unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Object
        let json = CString::new(r#"{"name": "test"}"#).unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_is_json_array_invalid() {
        use std::ffi::CString;

        let json = CString::new("not valid json").unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_parse_string_array_json() {
        use std::ffi::CString;

        let input = CString::new(r#"["a", "b", "c"]"#).unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, r#"["a","b","c"]"#);
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_string_array_comma_separated() {
        use std::ffi::CString;

        let input = CString::new("a, b, c").unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, r#"["a","b","c"]"#);
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_string_array_empty() {
        use std::ffi::CString;

        let input = CString::new("").unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "[]");
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_string_array_single() {
        use std::ffi::CString;

        let input = CString::new("single").unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, r#"["single"]"#);
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Duration parsing tests

    #[test]
    fn test_parse_go_duration_seconds() {
        assert_eq!(parse_go_duration("30s").unwrap(), 30_000_000_000);
        assert_eq!(parse_go_duration("1s").unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_minutes() {
        assert_eq!(parse_go_duration("5m").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(parse_go_duration("1m").unwrap(), 60_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_hours() {
        assert_eq!(parse_go_duration("1h").unwrap(), 3600 * 1_000_000_000);
        assert_eq!(parse_go_duration("2h").unwrap(), 2 * 3600 * 1_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_combined() {
        assert_eq!(parse_go_duration("1h30m").unwrap(), 5400 * 1_000_000_000);
        assert_eq!(
            parse_go_duration("2h45m30s").unwrap(),
            (2 * 3600 + 45 * 60 + 30) * 1_000_000_000
        );
    }

    #[test]
    fn test_parse_go_duration_milliseconds() {
        assert_eq!(parse_go_duration("300ms").unwrap(), 300_000_000);
        assert_eq!(parse_go_duration("1500ms").unwrap(), 1_500_000_000);
    }

    #[test]
    fn test_parse_go_duration_microseconds() {
        assert_eq!(parse_go_duration("100us").unwrap(), 100_000);
        assert_eq!(parse_go_duration("100µs").unwrap(), 100_000);
    }

    #[test]
    fn test_parse_go_duration_nanoseconds() {
        assert_eq!(parse_go_duration("1000ns").unwrap(), 1000);
    }

    #[test]
    fn test_parse_go_duration_negative() {
        assert_eq!(parse_go_duration("-30s").unwrap(), -30_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_float() {
        assert_eq!(
            parse_go_duration("1.5h").unwrap(),
            (1.5 * 3600.0 * 1e9) as i64
        );
        assert_eq!(parse_go_duration("0.5s").unwrap(), 500_000_000);
    }

    #[test]
    fn test_parse_go_duration_zero() {
        assert_eq!(parse_go_duration("0").unwrap(), 0);
        assert_eq!(parse_go_duration("").unwrap(), 0);
    }

    #[test]
    fn test_parse_go_duration_plain_integer() {
        assert_eq!(parse_go_duration("30").unwrap(), 30_000_000_000);
        assert_eq!(parse_go_duration("60").unwrap(), 60_000_000_000);
        assert_eq!(parse_go_duration("-30").unwrap(), -30_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_invalid() {
        assert!(parse_go_duration("invalid").is_err());
        assert!(parse_go_duration("30x").is_err());
    }

    #[test]
    fn test_parse_duration_ffi() {
        use std::ffi::CString;

        let input = CString::new("30s").unwrap();
        let result = unsafe { parse_duration(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "30000000000");
        unsafe { crate::types::defra_free_string(result.value) };
    }
}
