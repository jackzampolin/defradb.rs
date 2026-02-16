use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::error::{Error, Result};

/// Manage SDL (Schema Definition Language)
#[derive(Args, Debug)]
pub struct SdlArgs {
    #[command(subcommand)]
    pub command: SdlCommand,
}

/// SDL subcommands
#[derive(Subcommand, Debug)]
pub enum SdlCommand {
    /// Generate SDL from input files
    Generate(SdlGenerateArgs),
}

/// Arguments for sdl generate command
#[derive(Args, Debug)]
pub struct SdlGenerateArgs {
    /// Input files (use - for stdin)
    #[arg(value_name = "FILES")]
    pub files: Vec<PathBuf>,

    /// Output file path (use - for stdout)
    #[arg(long, short = 'o', default_value = "schema.gen.graphql")]
    pub output: PathBuf,

    /// Overwrite existing output file
    #[arg(long, short = 'y')]
    pub overwrite: bool,
}

impl SdlArgs {
    pub fn execute(&self) -> Result<()> {
        match &self.command {
            SdlCommand::Generate(args) => args.execute(),
        }
    }
}

impl SdlGenerateArgs {
    pub fn execute(&self) -> Result<()> {
        if self.files.is_empty() {
            return Err(Error::MissingInput(
                "at least one input file is required (use - for stdin)".into(),
            ));
        }

        // Read all input files
        let mut combined_sdl = String::new();
        for path in &self.files {
            let content = if path.as_os_str() == "-" {
                use std::io::Read;
                let mut buf = String::new();
                std::io::stdin()
                    .read_to_string(&mut buf)
                    .map_err(|e| Error::Server(format!("failed to read stdin: {}", e)))?;
                buf
            } else {
                std::fs::read_to_string(path).map_err(|e| Error::ReadFile {
                    path: path.clone(),
                    source: e,
                })?
            };
            combined_sdl.push_str(&content);
            combined_sdl.push('\n');
        }

        // Parse input SDL into collection versions
        let collections = query::parse_sdl(&combined_sdl)
            .map_err(|e| Error::Server(format!("SDL parse error: {}", e)))?;

        if collections.is_empty() {
            return Err(Error::Server(
                "no type definitions found in input SDL".into(),
            ));
        }

        // Generate full schema for each collection
        let collection_refs: Vec<&schema::CollectionVersion> = collections.iter().collect();
        let mut output_parts = Vec::new();

        for collection in &collections {
            let schema = query::schema_gen::generate_schema(collection, &collection_refs)
                .map_err(|e| Error::Server(format!("schema generation error: {}", e)))?;

            output_parts.push(schema.object_type.to_sdl());
            output_parts.push(schema.create_input.to_sdl());
            output_parts.push(schema.update_input.to_sdl());
            output_parts.push(schema.filter_input.to_sdl());
            output_parts.push(schema.order_input.to_sdl());
        }

        // Generate Query and Mutation types
        let query_type = query::schema_gen::generate_query_type(&collection_refs);
        let mutation_type = query::schema_gen::generate_mutation_type(&collection_refs);
        output_parts.push(query_type.to_sdl());
        output_parts.push(mutation_type.to_sdl());

        let output_sdl = output_parts.join("\n\n");

        // Write output
        if self.output.as_os_str() == "-" {
            println!("{}", output_sdl);
        } else {
            if self.output.exists() && !self.overwrite {
                return Err(Error::Server(format!(
                    "output file {} already exists (use --overwrite to replace)",
                    self.output.display()
                )));
            }
            std::fs::write(&self.output, &output_sdl).map_err(|e| {
                Error::Server(format!(
                    "failed to write output file {}: {}",
                    self.output.display(),
                    e
                ))
            })?;
            eprintln!(
                "Generated schema written to {} ({} types)",
                self.output.display(),
                collections.len()
            );
        }

        Ok(())
    }
}
