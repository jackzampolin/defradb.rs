#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Quick,
    Full,
}

impl Profile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
        }
    }
}
