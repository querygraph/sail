//! The sixteen upstream SQL names and ten separately named configurable helpers.
use crate::{matrix::MatrixMetric, token::TokenMetric};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Function {
    Jaccard,
    SorensenDice,
    OverlapCoefficient,
    Cosine,
    BraunBlanquet,
    MongeElkan,
    Levenshtein,
    LcsSimilarity,
    Jaro,
    JaroWinkler,
    NeedlemanWunsch,
    SmithWaterman,
    AffineGap,
    Soundex,
    RefinedSoundex,
    DoubleMetaphone,
}

impl Function {
    pub const ALL: [Self; 16] = [
        Self::Jaccard,
        Self::SorensenDice,
        Self::OverlapCoefficient,
        Self::Cosine,
        Self::BraunBlanquet,
        Self::MongeElkan,
        Self::Levenshtein,
        Self::LcsSimilarity,
        Self::Jaro,
        Self::JaroWinkler,
        Self::NeedlemanWunsch,
        Self::SmithWaterman,
        Self::AffineGap,
        Self::Soundex,
        Self::RefinedSoundex,
        Self::DoubleMetaphone,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Jaccard => "ss_jaccard",
            Self::SorensenDice => "ss_sorensen_dice",
            Self::OverlapCoefficient => "ss_overlap_coefficient",
            Self::Cosine => "ss_cosine",
            Self::BraunBlanquet => "ss_braun_blanquet",
            Self::MongeElkan => "ss_monge_elkan",
            Self::Levenshtein => "ss_levenshtein",
            Self::LcsSimilarity => "ss_lcs_similarity",
            Self::Jaro => "ss_jaro",
            Self::JaroWinkler => "ss_jaro_winkler",
            Self::NeedlemanWunsch => "ss_needleman_wunsch",
            Self::SmithWaterman => "ss_smith_waterman",
            Self::AffineGap => "ss_affine_gap",
            Self::Soundex => "ss_soundex",
            Self::RefinedSoundex => "ss_refined_soundex",
            Self::DoubleMetaphone => "ss_double_metaphone",
        }
    }

    pub fn configured_name(self) -> Option<&'static str> {
        match self {
            Self::Jaccard => Some("ss_jaccard_with_options"),
            Self::SorensenDice => Some("ss_sorensen_dice_with_options"),
            Self::OverlapCoefficient => Some("ss_overlap_coefficient_with_options"),
            Self::Cosine => Some("ss_cosine_with_options"),
            Self::BraunBlanquet => Some("ss_braun_blanquet_with_options"),
            Self::MongeElkan => Some("ss_monge_elkan_with_options"),
            Self::JaroWinkler => Some("ss_jaro_winkler_with_options"),
            Self::NeedlemanWunsch => Some("ss_needleman_wunsch_with_options"),
            Self::SmithWaterman => Some("ss_smith_waterman_with_options"),
            Self::AffineGap => Some("ss_affine_gap_with_options"),
            _ => None,
        }
    }

    pub fn phonetic(self) -> bool {
        matches!(
            self,
            Self::Soundex | Self::RefinedSoundex | Self::DoubleMetaphone
        )
    }

    pub fn token(self) -> Option<TokenMetric> {
        match self {
            Self::Jaccard => Some(TokenMetric::Jaccard),
            Self::SorensenDice => Some(TokenMetric::SorensenDice),
            Self::OverlapCoefficient => Some(TokenMetric::OverlapCoefficient),
            Self::Cosine => Some(TokenMetric::Cosine),
            Self::BraunBlanquet => Some(TokenMetric::BraunBlanquet),
            Self::MongeElkan => Some(TokenMetric::MongeElkan),
            _ => None,
        }
    }

    pub fn matrix(self) -> Option<MatrixMetric> {
        match self {
            Self::Levenshtein => Some(MatrixMetric::Levenshtein),
            Self::LcsSimilarity => Some(MatrixMetric::LcsSimilarity),
            Self::Jaro => Some(MatrixMetric::Jaro),
            Self::JaroWinkler => Some(MatrixMetric::JaroWinkler),
            Self::NeedlemanWunsch => Some(MatrixMetric::NeedlemanWunsch),
            Self::SmithWaterman => Some(MatrixMetric::SmithWaterman),
            Self::AffineGap => Some(MatrixMetric::AffineGap),
            _ => None,
        }
    }
}
