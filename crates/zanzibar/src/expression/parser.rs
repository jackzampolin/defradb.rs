use super::RelationExpression;
use crate::error::{Error, Result};

impl RelationExpression {
    /// Parse an expression from string format.
    ///
    /// Grammar:
    /// - `_this` -> This
    /// - `relation_name` -> ComputedUserset
    /// - `relation->computed` -> TupleToUserset
    /// - `expr + expr` -> Union
    /// - `expr & expr` -> Intersection
    /// - `expr - expr` -> Difference (note: "->" is NOT a difference operator)
    ///
    /// All operators have equal precedence and are evaluated left-to-right,
    /// matching Go zanzi behavior. Use parentheses to override.
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(Error::InvalidExpression("empty expression".into()));
        }

        if input.starts_with('(') && input.ends_with(')') && is_fully_parenthesized(input) {
            return Self::parse(&input[1..input.len() - 1]);
        }

        if let Some((pos, op)) = find_rightmost_operator(input) {
            let left = Self::parse(&input[..pos])?;
            let right = Self::parse(&input[pos + 1..])?;

            return match op {
                '+' => Ok(merge_union(left, right)),
                '&' => Ok(merge_intersection(left, right)),
                '-' => Ok(Self::Difference {
                    base: Box::new(left),
                    subtract: Box::new(right),
                }),
                _ => unreachable!(),
            };
        }

        parse_term(input)
    }
}

fn is_fully_parenthesized(input: &str) -> bool {
    if !input.starts_with('(') || !input.ends_with(')') {
        return false;
    }

    let mut depth = 0;
    let chars: Vec<char> = input.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && i < chars.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

fn find_rightmost_operator(input: &str) -> Option<(usize, char)> {
    let mut depth = 0;
    let chars: Vec<char> = input.chars().collect();
    let mut rightmost: Option<(usize, char)> = None;

    for i in 0..chars.len() {
        match chars[i] {
            '(' => depth += 1,
            ')' => depth -= 1,
            '+' | '&' if depth == 0 => {
                rightmost = Some((i, chars[i]));
            }
            '-' if depth == 0 => {
                if i + 1 < chars.len() && chars[i + 1] == '>' {
                    continue;
                }
                rightmost = Some((i, '-'));
            }
            _ => {}
        }
    }
    rightmost
}

fn parse_term(input: &str) -> Result<RelationExpression> {
    let input = input.trim();

    if input == "_this" {
        return Ok(RelationExpression::This);
    }

    if let Some(arrow_pos) = input.find("->") {
        let tuple_relation = input[..arrow_pos].trim();
        let computed_relation = input[arrow_pos + 2..].trim();

        if tuple_relation.is_empty() {
            return Err(Error::InvalidExpression(
                "empty tuple relation in TupleToUserset".into(),
            ));
        }
        if computed_relation.is_empty() {
            return Err(Error::InvalidExpression(
                "empty computed relation in TupleToUserset".into(),
            ));
        }

        validate_identifier(tuple_relation)?;
        validate_identifier(computed_relation)?;

        return Ok(RelationExpression::TupleToUserset {
            tuple_relation: tuple_relation.into(),
            computed_relation: computed_relation.into(),
        });
    }

    validate_identifier(input)?;
    Ok(RelationExpression::ComputedUserset {
        relation: input.into(),
    })
}

fn validate_identifier(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::InvalidExpression("empty identifier".into()));
    }

    let first = s.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(Error::InvalidExpression(format!(
            "identifier must start with letter or underscore: '{}'",
            s
        )));
    }

    for c in s.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(Error::InvalidExpression(format!(
                "invalid character '{}' in identifier: '{}'",
                c, s
            )));
        }
    }

    Ok(())
}

fn merge_union(left: RelationExpression, right: RelationExpression) -> RelationExpression {
    let mut exprs = Vec::new();

    match left {
        RelationExpression::Union(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    match right {
        RelationExpression::Union(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    RelationExpression::Union(exprs)
}

fn merge_intersection(left: RelationExpression, right: RelationExpression) -> RelationExpression {
    let mut exprs = Vec::new();

    match left {
        RelationExpression::Intersection(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    match right {
        RelationExpression::Intersection(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    RelationExpression::Intersection(exprs)
}
