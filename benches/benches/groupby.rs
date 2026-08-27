use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use query::mapper::GroupBy;
use query::plan::{GroupAlias, GroupByNode};
use query::{Doc, DocumentMapping, PlanNode};
use serde_json::Value as JsonValue;
use std::hint::black_box;

mod common;

const GROUP_INDEX: usize = 4;

#[derive(Clone)]
struct GroupByCase {
    docs: Vec<Doc>,
    mapping: DocumentMapping,
    group_by: GroupBy,
}

impl GroupByCase {
    fn new(doc_count: usize, group_count: usize) -> Self {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "team");
        mapping.add(2, "city");
        mapping.add(3, "score");
        mapping.add(GROUP_INDEX, "GROUP");

        let mut child_mapping = DocumentMapping::new();
        child_mapping.add(0, "_docID");
        child_mapping.add_render_key(0, "_docID");
        child_mapping.add(1, "team");
        child_mapping.add_render_key(1, "team");
        child_mapping.add(2, "city");
        child_mapping.add_render_key(2, "city");
        child_mapping.add(3, "score");
        child_mapping.add_render_key(3, "score");
        mapping.set_child_at(GROUP_INDEX, child_mapping);

        let docs = (0..doc_count)
            .map(|index| {
                let team = format!("team-{:02}", index % group_count);
                let city = format!("city-{:02}", (index / group_count) % 11);
                Doc::with_fields(vec![
                    Some(JsonValue::String(format!("doc-{index:04}"))),
                    Some(JsonValue::String(team)),
                    Some(JsonValue::String(city)),
                    Some(JsonValue::from((index % 100) as u64)),
                ])
            })
            .collect();

        Self {
            docs,
            mapping,
            group_by: GroupBy::new(vec!["team".to_string()]),
        }
    }

    fn make_node(&self) -> GroupByNode {
        GroupByNode::new(
            Box::new(VecPlanNode::new(self.docs.clone(), self.mapping.clone())),
            self.group_by.clone(),
            self.mapping.clone(),
        )
        .with_group_aliases(vec![GroupAlias {
            index: GROUP_INDEX,
            filter: None,
            limit: None,
            order: None,
            doc_ids: None,
        }])
    }

    fn make_key_node(&self) -> GroupByNode {
        GroupByNode::new(
            Box::new(VecPlanNode::new(Vec::new(), self.mapping.clone())),
            self.group_by.clone(),
            self.mapping.clone(),
        )
    }
}

#[derive(Clone)]
struct RenderCase {
    docs: Vec<Doc>,
    mapping: DocumentMapping,
}

impl RenderCase {
    fn new(doc_count: usize, field_count: usize) -> Self {
        let mut mapping = DocumentMapping::new();

        for field_index in 0..field_count {
            let field_name = format!("field_{field_index}");
            mapping.add(field_index, field_name.clone());
            mapping.add_render_key(field_index, field_name);
        }

        let docs = (0..doc_count)
            .map(|doc_index| {
                let fields = (0..field_count)
                    .map(|field_index| {
                        if field_index % 2 == 0 {
                            Some(JsonValue::String(format!(
                                "doc-{doc_index:04}-field-{field_index:02}"
                            )))
                        } else {
                            Some(JsonValue::from((doc_index + field_index) as u64))
                        }
                    })
                    .collect();
                Doc::with_fields(fields)
            })
            .collect();

        Self { docs, mapping }
    }
}

struct VecPlanNode {
    docs: Vec<Doc>,
    mapping: DocumentMapping,
    position: usize,
    current_doc: Doc,
}

impl VecPlanNode {
    fn new(docs: Vec<Doc>, mapping: DocumentMapping) -> Self {
        Self {
            docs,
            mapping,
            position: 0,
            current_doc: Doc::default(),
        }
    }
}

#[async_trait]
impl PlanNode for VecPlanNode {
    async fn init(&mut self) -> query::Result<()> {
        self.position = 0;
        self.current_doc = Doc::default();
        Ok(())
    }

    async fn start(&mut self) -> query::Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> query::Result<bool> {
        if self.position >= self.docs.len() {
            return Ok(false);
        }

        self.current_doc = self.docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> query::Result<()> {
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.mapping
    }

    fn kind(&self) -> &'static str {
        "benchVecPlanNode"
    }
}

fn execute_groupby(node: &mut GroupByNode) {
    common::shared_runtime().block_on(async {
        node.init().await.unwrap();
        let mut yielded = 0usize;
        while node.next().await.unwrap() {
            black_box(node.value());
            yielded += 1;
        }
        black_box(yielded);
        node.close().await.unwrap();
    });
}

fn execute_render(case: &RenderCase) {
    black_box(GroupByNode::render_docs_for_bench(
        &case.docs,
        &case.mapping.render_keys,
        None,
    ));
}

fn bench_groupby(c: &mut Criterion) {
    let mut group = c.benchmark_group("groupby");
    let render_cases = [
        ("10_docs_2_groups", GroupByCase::new(10, 2)),
        ("100_docs_5_groups", GroupByCase::new(100, 5)),
        ("1000_docs_10_groups", GroupByCase::new(1000, 10)),
    ];

    for (name, case) in &render_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), case, |b, case| {
            b.iter_batched_ref(|| case.make_node(), execute_groupby, BatchSize::LargeInput);
        });
    }

    let key_case = GroupByCase::new(1000, 10);
    let key_node = key_case.make_key_node();
    group.bench_function(BenchmarkId::from_parameter("key_generation_1000"), |b| {
        b.iter(|| {
            for doc in &key_case.docs {
                black_box(key_node.generate_key_for_doc(black_box(doc)).unwrap());
            }
        });
    });

    let render_case = RenderCase::new(1000, 10);
    group.bench_with_input(
        BenchmarkId::from_parameter("render_only_1000_docs_10_fields"),
        &render_case,
        |b, case| {
            b.iter(|| execute_render(black_box(case)));
        },
    );

    group.finish();
}

criterion_group!(benches, bench_groupby);
criterion_main!(benches);
