use std::collections::HashSet;

use crate::error::Result;
use crate::planner::ExecInfo;

use super::node::TypeJoinMany;

impl TypeJoinMany {
    pub(super) fn reset_init_state(&mut self) {
        self.exec_info = ExecInfo::default();
        self.child_exec_info = ExecInfo::default();
        self.go_child_metrics.reset();
        self.child_limit_iterations = 0;
        self.total_children_in_cache = 0;
        self.total_fields_per_scan = 0;
        self.child_scan_order.clear();
        self.filter_child_cache.clear();
    }

    pub(super) async fn build_filter_child_cache(
        &mut self,
        parent_scope: Option<&HashSet<String>>,
    ) -> Result<Option<ExecInfo>> {
        let Some(filter_plan) = self.filter_child_plan.as_mut() else {
            return Ok(None);
        };

        filter_plan.init().await?;
        filter_plan.start().await?;

        while filter_plan.next().await? {
            let doc = filter_plan.value();
            if let Some(fk_idx) = self.filter_child_fk_index {
                if let Some(fk) = doc.get(fk_idx).and_then(|v| v.as_str()) {
                    if parent_scope
                        .map(|parent_doc_ids| parent_doc_ids.contains(fk))
                        .unwrap_or(true)
                    {
                        self.filter_child_cache
                            .entry(fk.to_string())
                            .or_default()
                            .push(doc.deep_clone());
                    }
                }
            }
        }

        let info = filter_plan.exec_info();
        filter_plan.close().await?;

        Ok(Some(info))
    }

    pub(super) fn filter_child_doc_count(&self) -> usize {
        self.filter_child_cache
            .values()
            .map(|docs| docs.len())
            .sum()
    }
}
