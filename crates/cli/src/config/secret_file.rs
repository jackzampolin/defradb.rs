use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::Path;

use crate::error::{Error, Result};

pub(super) fn load(path: &Path) -> Result<()> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(Error::ReadSecretFile {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let mut variables = HashMap::new();
    for (index, line) in contents.lines().enumerate() {
        let line_number = index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = line
            .strip_prefix("export ")
            .map(str::trim_start)
            .unwrap_or(line);
        let (key, value) = line.split_once('=').ok_or_else(|| Error::ParseSecretFile {
            path: path.to_path_buf(),
            line: line_number,
        })?;
        let key = key.trim();
        if key.is_empty()
            || !key.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '.')
            })
        {
            return Err(Error::ParseSecretFile {
                path: path.to_path_buf(),
                line: line_number,
            });
        }

        variables.insert(
            key.to_owned(),
            parse_value(value.trim_start()).ok_or_else(|| Error::ParseSecretFile {
                path: path.to_path_buf(),
                line: line_number,
            })?,
        );
    }

    for (key, value) in variables {
        if env::var_os(&key).is_none() {
            env::set_var(key, value);
        }
    }

    Ok(())
}

fn parse_value(value: &str) -> Option<String> {
    let Some(quote @ ('\'' | '"')) = value.chars().next() else {
        let comment = value
            .char_indices()
            .find(|(index, character)| {
                *character == '#'
                    && value[..*index]
                        .chars()
                        .next_back()
                        .is_some_and(char::is_whitespace)
            })
            .map(|(index, _)| index)
            .unwrap_or(value.len());
        return Some(value[..comment].trim().to_owned());
    };

    let mut escaped = false;
    for (index, character) in value.char_indices().skip(1) {
        if character == quote && !escaped {
            let remainder = value[index + character.len_utf8()..].trim();
            if !remainder.is_empty() && !remainder.starts_with('#') {
                return None;
            }

            let quoted = &value[1..index];
            return Some(if quote == '"' {
                unescape_double_quoted(quoted)
            } else {
                quoted.to_owned()
            });
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }

    None
}

fn unescape_double_quoted(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            result.push(character);
            continue;
        }

        match characters.next() {
            Some('n') => result.push('\n'),
            Some('r') => result.push('\r'),
            Some(character) => result.push(character),
            None => result.push('\\'),
        }
    }
    result
}
