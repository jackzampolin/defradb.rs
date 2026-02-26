#[derive(Debug, Clone)]
pub struct ForeignKeyInfo {
    pub constraint_name: String,
    pub from_table: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
    pub on_delete_cascade: bool,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_name: String,
    pub table_name: String,
    #[allow(dead_code)]
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyInfo {
    pub table_name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedForeignKey {
    pub constraint_name: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
    pub on_delete_cascade: bool,
}

#[derive(Default, Debug)]
pub struct DdlMetadata {
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub primary_keys: Vec<PrimaryKeyInfo>,
}

impl DdlMetadata {
    pub fn add_foreign_key(&mut self, from_table: &str, fk: &ParsedForeignKey) {
        self.foreign_keys.push(ForeignKeyInfo {
            constraint_name: fk.constraint_name.clone(),
            from_table: from_table.to_string(),
            from_column: fk.from_column.clone(),
            to_table: fk.to_table.clone(),
            to_column: fk.to_column.clone(),
            on_delete_cascade: fk.on_delete_cascade,
        });
    }

    pub fn add_index(&mut self, info: IndexInfo) {
        self.indexes.push(info);
    }

    pub fn add_primary_key(&mut self, info: PrimaryKeyInfo) {
        self.primary_keys.push(info);
    }

    /// Return all FK relationships where the parent is `table_name` and cascade is enabled.
    pub fn cascade_children_of(&self, table_name: &str) -> Vec<&ForeignKeyInfo> {
        self.foreign_keys
            .iter()
            .filter(|fk| fk.to_table == table_name && fk.on_delete_cascade)
            .collect()
    }

    pub fn primary_key_for(&self, table_name: &str) -> Option<&PrimaryKeyInfo> {
        self.primary_keys
            .iter()
            .find(|pk| pk.table_name == table_name)
    }
}
