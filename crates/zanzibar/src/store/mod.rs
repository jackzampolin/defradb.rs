mod memory;
mod traits;

pub use memory::MemoryZanzibarStore;
pub use traits::ZanzibarStore;

#[derive(Debug, Clone, Default)]
pub struct StorePolicyOptions {
    pub validate: bool,
    pub enforce_dpi: bool,
}

impl StorePolicyOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_validation(mut self) -> Self {
        self.validate = true;
        self
    }

    pub fn with_dpi_enforcement(mut self) -> Self {
        self.enforce_dpi = true;
        self
    }
}
