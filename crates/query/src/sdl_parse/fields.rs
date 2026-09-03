//! Field and directive parsing methods
//!
//! Contains SdlParser methods for parsing field-level and type-level directives:
//! - `parse_field_directives()`, `check_directive_arguments()`
//! - `get_bool_with_warning()`, `get_string_with_warning()`, `get_int_with_warning()`
//! - `parse_type_directives()`, `parse_crdt_directive()`
//! - `parse_index_directive()`, `parse_vector_index_config()`, `parse_default_directive()`

use std::collections::BTreeMap;

use graphql_parser::schema::{Directive, Field};
use schema::CType;

use crate::error::{QueryError, Result};

use super::directives::{
    default_type_error, get_directive_arg, get_directive_string, get_directive_string_list,
    known_directive_arguments, IndexConfig, IndexDirection, ParsedDirectives,
    KNOWN_FIELD_DIRECTIVES, KNOWN_TYPE_DIRECTIVES,
};
use super::helpers::{
    graphql_schema_value_to_json, normalize_datetime_string, parse_graphql_type,
    parse_policy_directive,
};
use super::parser::{CompositeIndex, ParsedField, ParsedTypeDirectives, SdlParser};
use super::warnings::{DirectiveLocation, ParseWarning};

impl<'a> SdlParser<'a> {
    pub(super) fn parse_field(&mut self, field: &Field<'_, String>) -> Result<ParsedField> {
        let field_type = parse_graphql_type(&field.field_type);
        let directives =
            self.parse_field_directives(&field.directives, &field.name, &field_type.base_type)?;

        Ok(ParsedField {
            name: field.name.clone(),
            field_type,
            directives,
        })
    }

    pub(super) fn parse_field_directives(
        &mut self,
        directives: &[Directive<'_, String>],
        field_name: &str,
        field_type: &str,
    ) -> Result<ParsedDirectives> {
        let mut result = ParsedDirectives::default();
        let type_name = self.current_type.clone().unwrap_or_default();

        for directive in directives {
            let name = directive.name.as_str();

            // Check arguments for all known directives upfront. `@index` is
            // excluded: it validates its own, and refuses an unknown one
            // outright, so a warning here would only precede the error.
            if name != "index" && KNOWN_FIELD_DIRECTIVES.contains(&name) {
                self.check_directive_arguments(directive, &type_name, Some(field_name));
            }

            match name {
                "primary" => result.is_primary = true,
                "crdt" => result.crdt_type = Some(self.parse_crdt_directive(directive)?),
                "index" => match self.parse_index_directive(directive, Some(field_name))? {
                    ParsedIndex::Ordered(config) => result.index.push(config),
                    ParsedIndex::Vector(config) => result.vector_index.push(config),
                },
                "relation" => result.relation_name = get_directive_string(directive, "name"),
                "default" => {
                    result.default_value =
                        Some(self.parse_default_directive(directive, field_name, field_type)?);
                }
                "constraints" => {
                    if let Some(size) =
                        self.get_int_with_warning(directive, "size", &type_name, Some(field_name))
                    {
                        if size < 0 {
                            return Err(QueryError::parse(format!(
                                "@constraints size must be non-negative, got {}",
                                size
                            )));
                        }
                        result.size_constraint = Some(size as usize);
                    }
                }
                "encryptedIndex" => {
                    // Searchable encryption index
                    result.encrypted_index = true;
                }
                "fulltext" => {
                    let language = self.get_string_with_warning(
                        directive,
                        "language",
                        &type_name,
                        Some(field_name),
                    );
                    let k1 =
                        self.get_float_with_warning(directive, "k1", &type_name, Some(field_name));
                    let b =
                        self.get_float_with_warning(directive, "b", &type_name, Some(field_name));
                    result.fulltext = Some(super::directives::FullTextConfig { language, k1, b });
                }
                "immutable" => result.immutable = true,
                "embedding" => {
                    let provider = get_directive_string(directive, "provider").unwrap_or_default();
                    let model = get_directive_string(directive, "model").unwrap_or_default();
                    let url = get_directive_string(directive, "url").unwrap_or_default();
                    let fields = get_directive_string_list(directive, "fields");
                    let template = get_directive_string(directive, "template").unwrap_or_default();

                    result.embedding = Some(super::directives::EmbeddingConfig {
                        provider,
                        model,
                        url,
                        fields,
                        template,
                    });
                }
                "policy" => {
                    // Known but not yet implemented - emit warning so users know
                    self.warnings.push(ParseWarning::UnimplementedDirective {
                        directive_name: directive.name.clone(),
                        type_name: type_name.clone(),
                        field_name: Some(field_name.to_string()),
                    });
                }
                _ => {
                    // Unknown directive - emit warning for forward compatibility
                    self.warnings.push(ParseWarning::UnknownDirective {
                        directive_name: directive.name.clone(),
                        location: DirectiveLocation::Field,
                        type_name: type_name.clone(),
                        field_name: Some(field_name.to_string()),
                    });
                }
            }
        }

        Ok(result)
    }

    /// Check directive arguments and warn about unknown ones
    pub(super) fn check_directive_arguments(
        &mut self,
        directive: &Directive<'_, String>,
        type_name: &str,
        field_name: Option<&str>,
    ) {
        let known_args = known_directive_arguments(&directive.name);
        for (arg_name, _) in &directive.arguments {
            if !known_args.contains(&arg_name.as_str()) {
                self.warnings.push(ParseWarning::UnknownDirectiveArgument {
                    directive_name: directive.name.clone(),
                    argument_name: arg_name.clone(),
                    type_name: type_name.to_string(),
                    field_name: field_name.map(|s| s.to_string()),
                });
            }
        }
    }

    /// Get a boolean argument, emitting a warning if the type is wrong.
    ///
    /// Returns `None` if the argument is missing or has the wrong type.
    /// Callers typically use `.unwrap_or(default)` to apply a default value,
    /// meaning invalid types result in the default being used silently
    /// (with a warning emitted to `self.warnings`).
    pub(super) fn get_bool_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<bool> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::Boolean(b) => Some(*b),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "boolean".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get a string argument, emitting a warning if the type is wrong.
    ///
    /// Returns `None` if the argument is missing or has the wrong type.
    /// Accepts both String and Enum GraphQL values as valid strings.
    pub(super) fn get_string_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<String> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::String(s)
                | graphql_parser::schema::Value::Enum(s) => Some(s.clone()),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "string".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get an integer argument, emitting a warning if the type is wrong.
    ///
    /// Returns `None` if the argument is missing, has the wrong type, or
    /// is out of i64 range. A warning is emitted for type mismatches.
    pub(super) fn get_int_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<i64> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::Int(n) => n.as_i64().or_else(|| {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "integer (within i64 range)".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "integer".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get a float argument, emitting a warning if the type is wrong.
    pub(super) fn get_float_with_warning(
        &mut self,
        directive: &Directive<'_, String>,
        arg_name: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Option<f64> {
        if let Some(value) = get_directive_arg(directive, arg_name) {
            match value {
                graphql_parser::schema::Value::Float(f) => Some(*f),
                graphql_parser::schema::Value::Int(n) => n.as_i64().map(|i| i as f64),
                _ => {
                    self.warnings.push(ParseWarning::InvalidArgumentType {
                        directive_name: directive.name.clone(),
                        argument_name: arg_name.to_string(),
                        expected_type: "float".to_string(),
                        type_name: type_name.to_string(),
                        field_name: field_name.map(|s| s.to_string()),
                    });
                    None
                }
            }
        } else {
            None
        }
    }

    pub(super) fn parse_type_directives(
        &mut self,
        directives: &[Directive<'_, String>],
    ) -> Result<ParsedTypeDirectives> {
        let mut result = ParsedTypeDirectives::default();
        let type_name = self.current_type.clone().unwrap_or_default();

        for directive in directives {
            let name = directive.name.as_str();

            // Check arguments for all known directives upfront. `@index` is
            // excluded for the same reason as at field level: it validates its
            // own and refuses an unknown one.
            if name != "index" && KNOWN_TYPE_DIRECTIVES.contains(&name) {
                self.check_directive_arguments(directive, &type_name, None);
            }

            match name {
                "index" => {
                    // One parser for both levels, as the reference has. A
                    // type-level index names its fields rather than sitting on
                    // one, which is the only difference, and `None` here is
                    // what makes a vector configuration invalid.
                    let ParsedIndex::Ordered(config) =
                        self.parse_index_directive(directive, None)?
                    else {
                        return Err(QueryError::parse(
                            "@index vector is only valid on a field definition",
                        ));
                    };

                    if !config.includes.is_empty() {
                        result.indexes.push(CompositeIndex {
                            fields: config.includes,
                            name: config.name,
                            unique: config.unique,
                        });
                    }
                }
                "materialized" => {
                    result.is_materialized = self
                        .get_bool_with_warning(directive, "if", &type_name, None)
                        .unwrap_or(true);
                }
                "downsample" => {
                    let interval = match get_directive_arg(directive, "interval") {
                        Some(graphql_parser::schema::Value::String(value))
                        | Some(graphql_parser::schema::Value::Enum(value)) => value.clone(),
                        Some(graphql_parser::schema::Value::Int(value)) => value
                            .as_i64()
                            .map(|value| value.to_string())
                            .ok_or_else(|| {
                                QueryError::parse(
                                    "@downsample directive requires an interval within i64 range",
                                )
                            })?,
                        Some(_) => {
                            return Err(QueryError::parse(
                                "@downsample directive requires a string or integer 'interval' argument",
                            ));
                        }
                        None => {
                            return Err(QueryError::parse(
                                "@downsample directive requires an 'interval' argument",
                            ));
                        }
                    };
                    if interval.trim().is_empty() {
                        return Err(QueryError::parse("@downsample interval must not be empty"));
                    }
                    let time_field = self
                        .get_string_with_warning(directive, "timeField", &type_name, None)
                        .ok_or_else(|| {
                            QueryError::parse(
                                "@downsample directive requires a string 'timeField' argument",
                            )
                        })?;
                    if time_field.trim().is_empty() {
                        return Err(QueryError::parse("@downsample timeField must not be empty"));
                    }
                    let retention =
                        self.get_string_with_warning(directive, "retention", &type_name, None);
                    if retention
                        .as_ref()
                        .is_some_and(|retention| retention.trim().is_empty())
                    {
                        return Err(QueryError::parse("@downsample retention must not be empty"));
                    }
                    result.is_materialized = true;
                    result.downsample_interval = Some(interval);
                    result.downsample_time_field = Some(time_field);
                    result.downsample_retention = retention;
                }
                "branchable" => {
                    result.is_branchable = self
                        .get_bool_with_warning(directive, "if", &type_name, None)
                        .unwrap_or(true);
                }
                "policy" => {
                    result.policy = Some(parse_policy_directive(directive)?);
                }
                _ => {
                    // Unknown directive - emit warning for forward compatibility
                    self.warnings.push(ParseWarning::UnknownDirective {
                        directive_name: directive.name.clone(),
                        location: DirectiveLocation::Type,
                        type_name: type_name.clone(),
                        field_name: None,
                    });
                }
            }
        }

        Ok(result)
    }

    pub(super) fn parse_crdt_directive(&self, directive: &Directive<'_, String>) -> Result<CType> {
        let type_name = get_directive_string(directive, "type")
            .ok_or_else(|| QueryError::parse("@crdt directive requires 'type' argument"))?;

        match type_name.to_lowercase().as_str() {
            "lwwregister" | "lww" | "lww_register" => Ok(CType::LwwRegister),
            "pncounter" | "counter" | "pn_counter" => Ok(CType::PnCounter),
            "pcounter" | "p_counter" => Ok(CType::PCounter),
            other => Err(QueryError::parse(format!(
                "Argument \"type\" has invalid value \"{}\"",
                other
            ))),
        }
    }

    /// Reads a field-level `@index`, which carries every index kind.
    ///
    /// The kind is selected by which configuration argument is present, exactly
    /// as the wire's `Kind`/`KindDescription` envelope does: `vector:` means
    /// vector, `ordered:` or any of the legacy top-level ordered arguments
    /// means ordered, and `kind:` names one directly. Two selectors that
    /// disagree are an error rather than a precedence rule, because a
    /// precedence rule would silently build the index the author did not ask
    /// for.
    pub(super) fn parse_index_directive(
        &mut self,
        directive: &Directive<'_, String>,
        field_name: Option<&str>,
    ) -> Result<ParsedIndex> {
        let type_name = self.current_type.clone().unwrap_or_default();
        let mut kind: Option<IndexKindSelector> = None;
        let mut name: Option<String> = None;
        let mut ordered = OrderedIndexArgs::default();
        let mut vector: Option<&SchemaValue<'_>> = None;

        for (arg_name, value) in &directive.arguments {
            match arg_name.as_str() {
                "name" => {
                    let (SchemaValue::String(text) | SchemaValue::Enum(text)) = value else {
                        return Err(QueryError::parse("@index name must be a string"));
                    };
                    name = Some(text.clone());
                }
                "kind" => {
                    let (SchemaValue::String(text) | SchemaValue::Enum(text)) = value else {
                        return Err(QueryError::parse("@index kind must be an index kind"));
                    };
                    // `vector` is deliberately not accepted here: a vector index
                    // needs at least its dimensions, so there is no default
                    // configuration for `kind:` to select. Refusing it names the
                    // problem, where accepting it would fail later on a missing
                    // dimension.
                    match text.as_str() {
                        "ordered" => select_index_kind(&mut kind, IndexKindSelector::Ordered)?,
                        other => {
                            return Err(QueryError::parse(format!(
                                "@index has no kind named '{other}'"
                            )))
                        }
                    }
                }
                "ordered" => {
                    select_index_kind(&mut kind, IndexKindSelector::Ordered)?;
                    let SchemaValue::Object(members) = value else {
                        return Err(QueryError::parse(
                            "@index ordered must be an object of parameters",
                        ));
                    };
                    for (member, member_value) in members {
                        self.read_ordered_index_property(
                            member,
                            member_value,
                            &mut ordered,
                            &mut kind,
                            &type_name,
                            field_name,
                        )?;
                    }
                }
                "vector" => {
                    select_index_kind(&mut kind, IndexKindSelector::Vector)?;
                    vector = Some(value);
                }
                other => {
                    self.read_ordered_index_property(
                        other,
                        value,
                        &mut ordered,
                        &mut kind,
                        &type_name,
                        field_name,
                    )?;
                }
            }
        }

        if let Some(value) = vector {
            // A vector index covers exactly one field, so it has no type-level
            // form. `field_name` being absent is exactly that case.
            if field_name.is_none() {
                return Err(QueryError::parse(
                    "@index vector is only valid on a field definition",
                ));
            }
            let mut config = self.parse_vector_index_config(value)?;
            config.name = name;
            return Ok(ParsedIndex::Vector(config));
        }

        // Resolved here rather than as each argument is read, because an
        // included field with no direction of its own inherits the directive's,
        // and `direction:` may be written after `includes:`.
        let default_descending = matches!(ordered.direction, IndexDirection::Desc);
        Ok(ParsedIndex::Ordered(IndexConfig {
            name,
            unique: ordered.unique,
            direction: ordered.direction,
            includes: ordered
                .includes
                .into_iter()
                .map(|(name, descending)| (name, descending.unwrap_or(default_descending)))
                .collect(),
        }))
    }

    /// Reads one ordered-index property, from either the nested `ordered:`
    /// block or a legacy top-level argument.
    ///
    /// The name is recognised before the kind is selected, so an unknown
    /// argument is reported as an unknown argument rather than as a kind
    /// conflict it only causes incidentally.
    ///
    /// Writing the same property from both places is refused: the two forms
    /// merge, and a merge that silently drops one of two explicit values is
    /// worse than a parse error.
    fn read_ordered_index_property(
        &mut self,
        name: &str,
        value: &SchemaValue<'_>,
        config: &mut OrderedIndexArgs,
        kind: &mut Option<IndexKindSelector>,
        type_name: &str,
        field_name: Option<&str>,
    ) -> Result<()> {
        let (written, expected_type) = match name {
            "unique" => (&mut config.has_unique, "Boolean"),
            "direction" => (&mut config.has_direction, "Ordering"),
            "includes" | "fields" => (&mut config.has_includes, "[IndexField]"),
            other => {
                return Err(QueryError::parse(format!(
                    "@index has no argument named '{other}'"
                )))
            }
        };
        if *written {
            return Err(QueryError::parse(format!("@index sets '{name}' twice")));
        }
        *written = true;
        select_index_kind(kind, IndexKindSelector::Ordered)?;

        match name {
            "unique" => match value {
                SchemaValue::Boolean(unique) => config.unique = *unique,
                _ => self.warn_index_argument_type(name, expected_type, type_name, field_name),
            },
            "direction" => match value {
                SchemaValue::String(text) | SchemaValue::Enum(text) => {
                    config.direction = match text.as_str() {
                        "DESC" | "desc" | "Descending" => IndexDirection::Desc,
                        _ => IndexDirection::Asc,
                    };
                }
                _ => self.warn_index_argument_type(name, expected_type, type_name, field_name),
            },
            _ => config.includes = read_includes_value(value),
        }
        Ok(())
    }

    fn warn_index_argument_type(
        &mut self,
        argument_name: &str,
        expected_type: &str,
        type_name: &str,
        field_name: Option<&str>,
    ) {
        self.warnings.push(ParseWarning::InvalidArgumentType {
            directive_name: "index".to_string(),
            argument_name: argument_name.to_string(),
            expected_type: expected_type.to_string(),
            type_name: type_name.to_string(),
            field_name: field_name.map(str::to_string),
        });
    }

    /// Reads the `vector: { ... }` configuration of `@index`.
    ///
    /// An unrecognised member is an error rather than an ignored typo: a
    /// silently dropped `efSearch` would build a differently-shaped index than
    /// the schema asks for, and nothing downstream could tell.
    ///
    /// `metric` sits inside each algorithm block rather than beside
    /// `dimensions`, matching the reference: `dimensions` describes the field,
    /// while the metric is one of the algorithm's own knobs and an algorithm
    /// may not rank by every metric.
    pub(super) fn parse_vector_index_config(
        &self,
        value: &SchemaValue<'_>,
    ) -> Result<super::directives::VectorIndexConfig> {
        use super::directives::{directive_u32, FlatConfig, HnswConfig, IvfPqConfig, SsgConfig};

        let SchemaValue::Object(members) = value else {
            return Err(QueryError::parse(
                "@index vector must be an object of parameters",
            ));
        };

        let mut config = super::directives::VectorIndexConfig::default();
        for (member, member_value) in members {
            match member.as_str() {
                "dimensions" => {
                    config.dimensions = Some(
                        directive_u32("dimensions", member_value)
                            .map_err(|message| QueryError::parse(format!("@index {message}")))?,
                    );
                }
                "alg" => {
                    let (SchemaValue::String(text) | SchemaValue::Enum(text)) = member_value else {
                        return Err(QueryError::parse("@index vector alg must be an algorithm"));
                    };
                    config.algorithm = Some(text.clone());
                }
                block if block == schema::VectorAlgorithm::Flat.sdl_block() => {
                    let mut block = FlatConfig::default();
                    for (name, value) in vector_block(member, member_value)? {
                        match name.as_str() {
                            "metric" => block.metric = Some(read_metric(member, value)?),
                            other => return Err(unknown_vector_member(member, other)),
                        }
                    }
                    config.flat = Some(block);
                }
                block if block == schema::VectorAlgorithm::Hnsw.sdl_block() => {
                    let mut block = HnswConfig::default();
                    for (name, value) in vector_block(member, member_value)? {
                        let slot = match name.as_str() {
                            "metric" => {
                                block.metric = Some(read_metric(member, value)?);
                                continue;
                            }
                            "M" => &mut block.m,
                            "efConstruction" => &mut block.ef_construction,
                            "efSearch" => &mut block.ef_search,
                            other => return Err(unknown_vector_member(member, other)),
                        };
                        *slot = Some(read_block_u32(name, value)?);
                    }
                    config.hnsw = Some(block);
                }
                block if block == schema::VectorAlgorithm::IvfPq.sdl_block() => {
                    let mut block = IvfPqConfig::default();
                    for (name, value) in vector_block(member, member_value)? {
                        let slot = match name.as_str() {
                            "metric" => {
                                block.metric = Some(read_metric(member, value)?);
                                continue;
                            }
                            "nlist" => &mut block.nlist,
                            "nprobe" => &mut block.nprobe,
                            "m" => &mut block.m,
                            "sampleBytes" => &mut block.sample_bytes,
                            other => return Err(unknown_vector_member(member, other)),
                        };
                        *slot = Some(read_block_u32(name, value)?);
                    }
                    config.ivfpq = Some(block);
                }
                block if block == schema::VectorAlgorithm::Ssg.sdl_block() => {
                    let mut block = SsgConfig::default();
                    for (name, value) in vector_block(member, member_value)? {
                        let slot = match name.as_str() {
                            "metric" => {
                                block.metric = Some(read_metric(member, value)?);
                                continue;
                            }
                            "R" => &mut block.r,
                            "angle" => &mut block.angle,
                            "pool" => &mut block.pool,
                            other => return Err(unknown_vector_member(member, other)),
                        };
                        *slot = Some(read_block_u32(name, value)?);
                    }
                    config.ssg = Some(block);
                }
                other => {
                    return Err(QueryError::parse(format!(
                        "@index vector has no argument named '{other}'"
                    )))
                }
            }
        }
        Ok(config)
    }

    pub(super) fn parse_default_directive(
        &self,
        directive: &Directive<'_, String>,
        field_name: &str,
        field_type: &str,
    ) -> Result<serde_json::Value> {
        // Match Go's supported scalar field types for @default(value:).
        let Some((name, value)) = directive.arguments.first() else {
            return Err(QueryError::parse(format!(
                "default value must specify one argument. Field: {}",
                field_name
            )));
        };

        // Multiple arguments not allowed
        if directive.arguments.len() > 1 {
            return Err(QueryError::parse(format!(
                "default value must specify one argument. Field: {}",
                field_name
            )));
        }

        if name != "value" {
            return Err(QueryError::parse(format!(
                "Unknown argument \"{}\" on directive \"@default\"",
                name
            )));
        }

        match field_type {
            "String" => match value {
                graphql_parser::schema::Value::String(s) => {
                    Ok(serde_json::Value::String(s.clone()))
                }
                other => Err(default_type_error(field_name, field_type, other)),
            },
            "Boolean" => match value {
                graphql_parser::schema::Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
                other => Err(default_type_error(field_name, field_type, other)),
            },
            "Int" => match value {
                graphql_parser::schema::Value::Int(n) => {
                    let int_val = n.as_i64().ok_or_else(|| {
                        QueryError::parse("@default int value is out of i64 range")
                    })?;
                    Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                }
                other => Err(default_type_error(field_name, field_type, other)),
            },
            "Float" | "Float64" => match value {
                graphql_parser::schema::Value::Float(f) => serde_json::Number::from_f64(*f)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        QueryError::parse(
                            "@default float value is not a valid JSON number (NaN or Infinity)",
                        )
                    }),
                // Accept Int values for Float (common in schemas where 10 means 10.0)
                graphql_parser::schema::Value::Int(n) => {
                    let int_val = n.as_i64().ok_or_else(|| {
                        QueryError::parse("@default float value is out of i64 range")
                    })?;
                    Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                }
                other => Err(default_type_error(field_name, field_type, other)),
            },
            "Float32" => match value {
                graphql_parser::schema::Value::Float(f) => {
                    let f32_val = *f as f32;
                    if f32_val.is_infinite() && !f.is_infinite() {
                        return Err(QueryError::parse(
                            "@default float32 value is out of f32 range",
                        ));
                    }
                    serde_json::Number::from_f64(f32_val as f64)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| {
                            QueryError::parse(
                                "@default float32 value is not a valid JSON number (NaN or Infinity)",
                            )
                        })
                }
                // Accept Int values for Float32 (common in schemas where 10 means 10.0)
                graphql_parser::schema::Value::Int(n) => {
                    let int_val = n.as_i64().ok_or_else(|| {
                        QueryError::parse("@default float32 value is out of i64 range")
                    })?;
                    Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                }
                other => Err(default_type_error(field_name, field_type, other)),
            },
            "DateTime" => match value {
                graphql_parser::schema::Value::String(s) => {
                    // Normalize to RFC3339 format to match Go's time.Time serialization.
                    // Go parses the datetime string into time.Time then formats it back,
                    // which drops trailing zeros in the fractional seconds.
                    // Parse as DateTime and re-format, or pass through if it's a special value.
                    let normalized = normalize_datetime_string(s);
                    Ok(serde_json::Value::String(normalized))
                }
                // Accept Enum for special values like UTC_NOW
                graphql_parser::schema::Value::Enum(s) => Ok(serde_json::Value::String(s.clone())),
                other => Err(default_type_error(field_name, field_type, other)),
            },
            "JSON" => {
                // JSON @default accepts various value types
                match value {
                    // String containing JSON - validate but store as string literal
                    // Go stores JSON defaults as strings and returns them as strings
                    graphql_parser::schema::Value::String(s) => {
                        // Validate the JSON is parseable
                        let _: serde_json::Value = serde_json::from_str(s).map_err(|e| {
                            QueryError::parse(format!("@default json contains invalid JSON: {}", e))
                        })?;
                        // Store as string literal to match Go behavior
                        Ok(serde_json::Value::String(s.clone()))
                    }
                    // Primitives and structured types are converted directly
                    graphql_parser::schema::Value::Int(n) => {
                        let int_val = n.as_i64().unwrap_or(0);
                        Ok(serde_json::Value::Number(serde_json::Number::from(int_val)))
                    }
                    graphql_parser::schema::Value::Float(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| {
                            QueryError::parse("@default json float is invalid (NaN or Infinity)")
                        }),
                    graphql_parser::schema::Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
                    graphql_parser::schema::Value::Null => {
                        Err(QueryError::parse("default value is invalid for type JSON"))
                    }
                    graphql_parser::schema::Value::Enum(s) => {
                        Ok(serde_json::Value::String(s.clone()))
                    }
                    // JSON array/object defaults are stored as serialized strings to match Go behavior
                    graphql_parser::schema::Value::List(arr) => {
                        let items: Vec<serde_json::Value> = arr
                            .iter()
                            .map(|v| graphql_schema_value_to_json(v))
                            .collect();
                        let json_array = serde_json::Value::Array(items);
                        Ok(serde_json::Value::String(
                            serde_json::to_string(&json_array).unwrap_or_default(),
                        ))
                    }
                    graphql_parser::schema::Value::Object(obj) => {
                        let items: serde_json::Map<String, serde_json::Value> = obj
                            .iter()
                            .map(|(k, v)| (k.clone(), graphql_schema_value_to_json(v)))
                            .collect();
                        let json_obj = serde_json::Value::Object(items);
                        Ok(serde_json::Value::String(
                            serde_json::to_string(&json_obj).unwrap_or_default(),
                        ))
                    }
                    graphql_parser::schema::Value::Variable(v) => {
                        Ok(serde_json::Value::String(format!("${}", v)))
                    }
                }
            }
            "Blob" => match value {
                graphql_parser::schema::Value::String(s) => {
                    Ok(serde_json::Value::String(s.clone()))
                }
                other => Err(default_type_error(field_name, field_type, other)),
            },
            _ => Err(QueryError::parse(format!(
                "default value is not allowed for this field type. Name: {}, Type: {}",
                field_name, field_type
            ))),
        }
    }
}

/// A `@index` argument value, aliased because the parser names it constantly.
type SchemaValue<'a> = graphql_parser::schema::Value<'a, String>;

/// What a `@index` invocation resolves to. One variant per index kind, mirroring
/// the wire's `Kind` discriminator, so a kind can never be inferred twice from
/// two different places.
pub(super) enum ParsedIndex {
    Ordered(IndexConfig),
    Vector(super::directives::VectorIndexConfig),
}

/// The kind an argument selects. Distinct from `schema::IndexKind` because the
/// selection is a parse-time fact about the directive, not a described index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexKindSelector {
    Ordered,
    Vector,
}

impl IndexKindSelector {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Vector => "vector",
        }
    }
}

/// Ordered-index arguments as written, before defaults are resolved.
///
/// `has_*` record whether a property was *written*, not what it resolved to.
/// The nested `ordered:` block and the legacy top-level arguments merge, so
/// setting one property from both places has to be an error rather than a
/// silent last-writer-wins. `includes` stays unparsed until the directive's
/// `direction:` is known, since an entry without its own direction inherits it.
#[derive(Default)]
struct OrderedIndexArgs {
    unique: bool,
    direction: IndexDirection,
    /// `None` on an entry means it named no direction and inherits the
    /// directive's, which may be written after `includes:`.
    includes: Vec<(String, Option<bool>)>,
    has_unique: bool,
    has_direction: bool,
    has_includes: bool,
}

/// Records the kind an argument selects, refusing a second, different one.
fn select_index_kind(
    current: &mut Option<IndexKindSelector>,
    selected: IndexKindSelector,
) -> Result<()> {
    match current {
        Some(existing) if *existing != selected => Err(QueryError::parse(format!(
            "@index cannot be both {} and {}",
            existing.as_str(),
            selected.as_str()
        ))),
        _ => {
            *current = Some(selected);
            Ok(())
        }
    }
}

/// The members of one algorithm block, or an error naming the block.
fn vector_block<'v, 'a>(
    block: &str,
    value: &'v SchemaValue<'a>,
) -> Result<&'v BTreeMap<String, SchemaValue<'a>>> {
    match value {
        SchemaValue::Object(members) => Ok(members),
        _ => Err(QueryError::parse(format!(
            "@index vector {block} must be an object of parameters"
        ))),
    }
}

fn read_metric(block: &str, value: &SchemaValue<'_>) -> Result<String> {
    match value {
        SchemaValue::String(text) | SchemaValue::Enum(text) => Ok(text.clone()),
        _ => Err(QueryError::parse(format!(
            "@index vector {block} metric must be a metric"
        ))),
    }
}

fn read_block_u32(name: &str, value: &SchemaValue<'_>) -> Result<u32> {
    super::directives::directive_u32(name, value)
        .map_err(|message| QueryError::parse(format!("@index vector {message}")))
}

fn unknown_vector_member(block: &str, member: &str) -> QueryError {
    QueryError::parse(format!(
        "@index vector {block} has no argument named '{member}'"
    ))
}

/// Reads an `includes` or `fields` list into (field_name, descending) pairs.
///
/// An entry is either the reference's object form, `{field: "name", direction:
/// DESC}`, or our bare string, which names the field and no direction.
///
/// `None` for an entry that named no direction: it inherits the directive's,
/// which the caller resolves once every argument has been read.
fn read_includes_value(value: &SchemaValue<'_>) -> Vec<(String, Option<bool>)> {
    let SchemaValue::List(items) = value else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| match item {
            SchemaValue::String(name) | SchemaValue::Enum(name) => Some((name.clone(), None)),
            SchemaValue::Object(obj) => {
                let field_name = obj.get("field").and_then(|v| match v {
                    SchemaValue::String(s) | SchemaValue::Enum(s) => Some(s.clone()),
                    _ => None,
                })?;
                let descending = obj.get("direction").and_then(|v| match v {
                    SchemaValue::String(s) | SchemaValue::Enum(s) => {
                        Some(matches!(s.as_str(), "DESC" | "desc" | "Descending"))
                    }
                    _ => None,
                });
                Some((field_name, descending))
            }
            _ => None,
        })
        .collect()
}
