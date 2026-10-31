/// Controls lowering while resolving SQL constant-name expressions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConstantContext {
    #[default]
    Ordinary,
    Identifier,
}

impl ConstantContext {
    /// Retain function provenance until the identifier's foldability check.
    pub(crate) fn retain_function_foldability(self) -> bool {
        match self {
            Self::Ordinary => false,
            Self::Identifier => true,
        }
    }
}
