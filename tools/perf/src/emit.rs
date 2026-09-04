//! The contract between a benchmark and the performance dashboard.
//!
//! A bench describes what it measured as one [`Family`]; the dashboard renders
//! any family without knowing its name. That is the point. A page with a
//! bespoke renderer per family silently draws nothing for a family nobody
//! wrote code for, and a blank section is not an error to a browser, so the
//! gap reaches the site instead of CI. Here the shape carries its own labels,
//! units and direction, so the renderer is one function and a new bench lands
//! on the page with no page edit at all.
//!
//! Records are appended to `$DEFRA_BENCH_OUT` as JSON Lines when it is set,
//! and land in `./bench-out/<family>.json` otherwise so a single bench can be
//! run standalone.

#![allow(dead_code)]

#[cfg(not(target_arch = "wasm32"))]
use std::fs::{self, File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use serde::Serialize;

/// How much a measurement can be believed.
///
/// Only ever downgraded: a bench that declares its own doubt keeps it, and the
/// load guard can add doubt but never remove it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Trust {
    /// Measured on a host certified quiet, or deterministic and immune to load.
    Clean,
    /// Measured, but on a host that was busy. Comparable only with care.
    Contaminated,
    /// Not measured in this run. Renders as a gap, never as a zero.
    Absent,
}

/// One measurement inside a group.
#[derive(Clone, Debug, Serialize)]
pub struct Row {
    pub name: String,
    pub value: f64,
    /// Extremes across repetitions, when the bench repeated the measurement.
    /// A comparison only calls a delta real when two ranges do not overlap, so
    /// a bench that can report these should.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Numeric x for the group's chart. A group whose rows all carry one is
    /// drawn as a line; a group without is drawn as a table only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<f64>,
}

impl Row {
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Row {
            name: name.into(),
            value,
            min: None,
            max: None,
            x: None,
        }
    }

    pub fn range(mut self, min: f64, max: f64) -> Self {
        self.min = Some(min);
        self.max = Some(max);
        self
    }

    pub fn at(mut self, x: f64) -> Self {
        self.x = Some(x);
        self
    }
}

/// A set of rows sharing a unit and a direction.
#[derive(Clone, Debug, Serialize)]
pub struct Group {
    pub name: String,
    pub unit: String,
    /// Carried rather than guessed from the unit: "MiB/s" is a rate to
    /// maximise and "MiB" is a footprint to minimise, and both contain "mib".
    pub lower_is_better: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_label: Option<String>,
    pub rows: Vec<Row>,
}

impl Group {
    pub fn higher_better(name: impl Into<String>, unit: impl Into<String>) -> Self {
        Group {
            name: name.into(),
            unit: unit.into(),
            lower_is_better: false,
            note: None,
            x_label: None,
            rows: Vec::new(),
        }
    }

    pub fn lower_better(name: impl Into<String>, unit: impl Into<String>) -> Self {
        Group {
            lower_is_better: true,
            ..Group::higher_better(name, unit)
        }
    }

    pub fn note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Label the x axis, which is also what makes the group draw as a chart.
    pub fn over(mut self, x_label: impl Into<String>) -> Self {
        self.x_label = Some(x_label.into());
        self
    }

    pub fn row(mut self, row: Row) -> Self {
        self.rows.push(row);
        self
    }

    pub fn rows(mut self, rows: impl IntoIterator<Item = Row>) -> Self {
        self.rows.extend(rows);
        self
    }
}

/// Everything one bench target measured.
#[derive(Clone, Debug, Serialize)]
pub struct Family {
    pub title: String,
    pub note: String,
    pub trust: Trust,
    /// A family a busy host cannot move: a byte count, an allocation count, a
    /// size. The load guard downgrades timing families and leaves these alone,
    /// so a run taken on a noisy runner still compares them.
    pub deterministic: bool,
    pub groups: Vec<Group>,
}

impl Family {
    pub fn new(title: impl Into<String>, note: impl Into<String>) -> Self {
        Family {
            title: title.into(),
            note: note.into(),
            trust: Trust::Clean,
            deterministic: false,
            groups: Vec::new(),
        }
    }

    /// Mark a family whose result does not depend on how busy the host was.
    pub fn deterministic(mut self) -> Self {
        self.deterministic = true;
        self
    }

    /// Declare doubt this bench knows about. The collector can add more.
    pub fn trust(mut self, trust: Trust) -> Self {
        self.trust = trust;
        self
    }

    pub fn group(mut self, group: Group) -> Self {
        self.groups.push(group);
        self
    }

    /// The one record line a family becomes, whatever carries it.
    ///
    /// A native bench appends this to a file; a browser has no file and prints
    /// it instead, behind a marker the runner greps for. Both go through here,
    /// so the two transports cannot disagree about the shape.
    pub fn record(&self, name: &str) -> String {
        serde_json::json!({ "family": name, "data": self }).to_string()
    }

    /// Append this family to the run's records.
    ///
    /// `name` is the stable key the dashboard joins on across runs, so it must
    /// not change once a run carrying it has been published.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn emit(self, name: &str) {
        let line = format!("{}\n", self.record(name));
        match std::env::var_os("DEFRA_BENCH_OUT") {
            Some(p) if !p.is_empty() => {
                let path = PathBuf::from(p);
                let mut f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
                f.write_all(line.as_bytes())
                    .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
            }
            _ => {
                let dir = PathBuf::from("bench-out");
                fs::create_dir_all(&dir)
                    .unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
                let path = dir.join(format!("{name}.json"));
                let mut f = File::create(&path)
                    .unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
                f.write_all(line.as_bytes())
                    .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
            }
        }
    }
}
