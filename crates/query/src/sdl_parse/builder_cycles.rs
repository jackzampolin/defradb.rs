//! Cycle detection for SDL builder (Tarjan's SCC algorithm).

use super::parser::SdlParser;

impl<'a> SdlParser<'a> {
    /// Detect types that form circular relation sets.
    /// Returns a map from type name to its sorted index within its connected cycle group.
    /// Types with circular relations use SelfKind with relative indices for CID generation.
    ///
    /// IMPORTANT: A cycle only exists if BOTH sides of a mutual reference are PRIMARY.
    /// If one side has @primary and the other doesn't, the side WITHOUT @primary is SECONDARY
    /// and gets empty FieldID (not included in CID calculation), breaking the cycle.
    ///
    /// IMPORTANT: Only types that are ACTUALLY part of a cycle are included.
    /// Different cycle groups (e.g., Employee self-ref) are treated as separate collection sets.
    #[allow(clippy::type_complexity)]
    pub(super) fn detect_collection_set(
        &self,
        type_names: &std::collections::HashSet<String>,
        primary_directives: &std::collections::HashMap<(String, String), bool>,
    ) -> (
        std::collections::HashMap<String, (i32, usize)>,
        Vec<Vec<String>>,
    ) {
        // Helper to check if a relation field from source->target is actually primary
        // (will be included in CID calculation)
        let is_field_primary = |source: &str, target: &str, is_array: bool| -> bool {
            if is_array {
                // Arrays are always secondary
                return false;
            }

            // Check if this field has @primary directive
            let has_primary = primary_directives
                .get(&(source.to_string(), target.to_string()))
                .copied()
                .unwrap_or(false);

            if has_primary {
                return true;
            }

            // Check if the counterpart (target->source) has @primary
            // If it does, this side is SECONDARY
            let counterpart_has_primary = primary_directives
                .get(&(target.to_string(), source.to_string()))
                .copied()
                .unwrap_or(false);

            if counterpart_has_primary {
                // Counterpart has @primary, so this side is secondary
                return false;
            }

            // Neither side has @primary - single-object relation defaults to primary
            true
        };

        // Build relation graph: which types reference which other types via PRIMARY relations only
        let mut references: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for (type_name, type_def) in &self.type_defs {
            let mut refs = std::collections::HashSet::new();
            for field in &type_def.fields {
                let target = &field.field_type.base_type;
                // Only consider relations to other types that are:
                // 1. In the current schema (type_names)
                // 2. Actually PRIMARY (will be included in CID)
                if type_names.contains(target)
                    && is_field_primary(type_name, target, field.field_type.is_list)
                {
                    refs.insert(target.clone());
                }
            }
            references.insert(type_name.clone(), refs);
        }

        // Find strongly connected components (SCCs) using Tarjan's algorithm.
        // An SCC is a maximal set where every node can reach every other node via
        // directed primary edges. This correctly detects cycles like A->B->C->D->A even
        // when not all edges are bidirectional.
        let components = find_sccs(&references);

        if components.is_empty() {
            return (std::collections::HashMap::new(), Vec::new());
        }

        // Build result: each type gets (relative_id, group_index)
        let mut relative_ids = std::collections::HashMap::new();
        let mut groups = Vec::new();
        for mut members in components {
            members.sort(); // Sort alphabetically within component
            let group_idx = groups.len();
            for (idx, name) in members.iter().enumerate() {
                relative_ids.insert(name.clone(), (idx as i32, group_idx));
            }
            groups.push(members);
        }

        (relative_ids, groups)
    }
}

/// Find strongly connected components in a directed graph using Tarjan's algorithm.
/// Returns only SCCs that represent actual cycles (2+ members, or self-referencing singletons).
fn find_sccs(
    graph: &std::collections::HashMap<String, std::collections::HashSet<String>>,
) -> Vec<Vec<String>> {
    struct TarjanState<'a> {
        graph: &'a std::collections::HashMap<String, std::collections::HashSet<String>>,
        type_list: Vec<String>,
        idx_map: std::collections::HashMap<String, usize>,
        index_counter: usize,
        stack: Vec<usize>,
        on_stack: Vec<bool>,
        indices: Vec<usize>,
        lowlinks: Vec<usize>,
        sccs: Vec<Vec<String>>,
    }

    impl TarjanState<'_> {
        fn visit(&mut self, v: usize) {
            self.indices[v] = self.index_counter;
            self.lowlinks[v] = self.index_counter;
            self.index_counter += 1;
            self.stack.push(v);
            self.on_stack[v] = true;

            if let Some(refs) = self.graph.get(&self.type_list[v]) {
                for target in refs {
                    if let Some(&w) = self.idx_map.get(target) {
                        if self.indices[w] == usize::MAX {
                            self.visit(w);
                            self.lowlinks[v] = self.lowlinks[v].min(self.lowlinks[w]);
                        } else if self.on_stack[w] {
                            self.lowlinks[v] = self.lowlinks[v].min(self.indices[w]);
                        }
                    }
                }
            }

            if self.lowlinks[v] == self.indices[v] {
                let mut scc = Vec::new();
                loop {
                    let w = self.stack.pop().expect("Tarjan SCC: v is on stack");
                    self.on_stack[w] = false;
                    scc.push(self.type_list[w].clone());
                    if w == v {
                        break;
                    }
                }
                if scc.len() > 1 {
                    self.sccs.push(scc);
                } else if scc.len() == 1 {
                    if let Some(refs) = self.graph.get(&scc[0]) {
                        if refs.contains(&scc[0]) {
                            self.sccs.push(scc);
                        }
                    }
                }
            }
        }
    }

    let mut type_list: Vec<String> = graph.keys().cloned().collect();
    type_list.sort();
    let n = type_list.len();
    let idx_map: std::collections::HashMap<String, usize> = type_list
        .iter()
        .enumerate()
        .map(|(i, s)| (s.clone(), i))
        .collect();

    let mut state = TarjanState {
        graph,
        type_list,
        idx_map,
        index_counter: 0,
        stack: Vec::new(),
        on_stack: vec![false; n],
        indices: vec![usize::MAX; n],
        lowlinks: vec![0; n],
        sccs: Vec::new(),
    };

    for i in 0..n {
        if state.indices[i] == usize::MAX {
            state.visit(i);
        }
    }

    state.sccs
}
