use serde::Deserialize;

use super::spark_root_lowercase;
use crate::utils::string::spark_case_insensitive_name_eq;

#[derive(Deserialize)]
struct Oracle {
    java_runtime_version: String,
    scalar_fnv1a64: String,
    strings: Vec<CodePoints>,
    enumeration_count: usize,
    enumeration_fnv1a64: String,
    random_count: usize,
    random_seed: String,
    random_fnv1a64: String,
    named: Vec<Units>,
}

#[derive(Deserialize)]
struct CodePoints {
    input: Vec<u32>,
    lower: Vec<u32>,
}

#[derive(Deserialize)]
struct Units {
    input: Vec<u16>,
    lower: Vec<u16>,
}

fn oracle() -> Oracle {
    #[expect(clippy::expect_used)]
    serde_json::from_str(include_str!("root_lowercase_oracle.json"))
        .expect("the JDK oracle fixture should parse")
}

fn text(code_points: &[u32]) -> String {
    code_points
        .iter()
        .filter_map(|&c| char::from_u32(c))
        .collect()
}

/// 64-bit FNV-1a over each output's UTF-8 followed by a zero byte, as the probes hash.
struct Fnv(u64);

impl Fnv {
    fn new() -> Self {
        Self(0xcbf29ce484222325)
    }

    fn feed(&mut self, output: &str) {
        for byte in output.bytes().chain(std::iter::once(0)) {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    fn hex(&self) -> String {
        format!("{:x}", self.0)
    }
}

#[test]
fn every_scalar_lowercases_as_on_the_pinned_jvm() {
    let oracle = oracle();
    assert!(oracle.java_runtime_version.starts_with("17.0.8"));
    let mut hash = Fnv::new();
    for code in 0..=0x10FFFF_u32 {
        if let Some(char) = char::from_u32(code) {
            hash.feed(&spark_root_lowercase(&char.to_string()));
        }
    }
    assert_eq!(hash.hex(), oracle.scalar_fnv1a64);
}

#[test]
fn designed_context_strings_match_the_pinned_jvm() {
    let oracle = oracle();
    assert!(oracle.strings.len() >= 200);
    for case in &oracle.strings {
        let input = text(&case.input);
        assert_eq!(
            spark_root_lowercase(&input),
            text(&case.lower),
            "JDK 17 ROOT lowercase of {input:?}"
        );
    }
}

#[test]
fn reviewed_counterexamples_match_the_pinned_jvm() {
    // Marks and format characters after a digit belong to the number, so
    // a following connector and sigma start a new word (Codex, CF-0019).
    let oracle = oracle();
    assert!(oracle.named.len() >= 10);
    for case in &oracle.named {
        let input = String::from_utf16_lossy(&case.input);
        assert_eq!(
            spark_root_lowercase(&input),
            String::from_utf16_lossy(&case.lower),
            "JDK 17 ROOT lowercase of {input:?}"
        );
    }
}

#[test]
fn review_enumeration_set_matches_the_pinned_jvm() {
    // Every suffix of length zero to five over A, 1, dot, apostrophe,
    // underscore, combining acute and ZWJ, in `AΣ`+suffix and
    // `A`+suffix+`Σ` frames, deduplicated in order: the set of the
    // independent context review.
    let oracle = oracle();
    let symbols = ['A', '1', '.', '\'', '_', '\u{301}', '\u{200D}'];
    let mut seen = std::collections::HashSet::new();
    let mut hash = Fnv::new();
    let mut count = 0;
    for length in 0..=5 {
        let total = symbols.len().pow(length);
        for ordinal in 0..total {
            let mut digits = vec![0; length as usize];
            let mut value = ordinal;
            for digit in digits.iter_mut().rev() {
                *digit = value % symbols.len();
                value /= symbols.len();
            }
            let suffix: String = digits.iter().map(|&d| symbols[d]).collect();
            for input in [format!("AΣ{suffix}"), format!("A{suffix}Σ")] {
                if seen.insert(input.clone()) {
                    hash.feed(&spark_root_lowercase(&input));
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, oracle.enumeration_count);
    assert_eq!(hash.hex(), oracle.enumeration_fnv1a64);
}

#[test]
fn seeded_random_context_strings_match_the_pinned_jvm() {
    // xorshift64* with the fixture's seed over the probe's alphabet,
    // lengths one to eight; the JVM hashed the same sequence.
    const ALPHABET: [u32; 62] = [
        0x41, 0x61, 0x31, 0x2E, 0x27, 0x5F, 0x2D, 0x22, 0x2C, 0x3A, 0x20, 0x09, 0x0A, 0x2019,
        0x2018, 0x00B7, 0x2027, 0x066B, 0x0301, 0x0345, 0x037A, 0x200D, 0x200C, 0x00AD, 0x200B,
        0x2060, 0xFE0F, 0x0391, 0x03A3, 0x03C3, 0x03C2, 0x03B1, 0x0130, 0x0131, 0x0049, 0x0069,
        0x02B0, 0x02C0, 0x02E0, 0x1D2C, 0x2160, 0x2170, 0x24B6, 0x24D0, 0x00B2, 0x0660, 0x1C90,
        0x10D0, 0x10400, 0x10428, 0x10570, 0x10597, 0x1C89, 0x1C8A, 0x3042, 0x30A2, 0x4E00, 0xFF21,
        0xFF41, 0xFF10, 0xFFFF, 0xFFFE,
    ];
    let oracle = oracle();
    let mut state = u64::from_str_radix(&oracle.random_seed, 16).unwrap_or_default();
    let mut step = || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545F4914F6CDD1D)
    };
    let mut hash = Fnv::new();
    for _ in 0..oracle.random_count {
        let length = 1 + usize::try_from(step() % 8).unwrap_or(0);
        let input: String = (0..length)
            .filter_map(|_| {
                let pick = usize::try_from(step() % ALPHABET.len() as u64).unwrap_or(0);
                char::from_u32(ALPHABET[pick])
            })
            .collect();
        hash.feed(&spark_root_lowercase(&input));
    }
    assert_eq!(hash.hex(), oracle.random_fnv1a64);
}

/// Opt-in replay of an external JVM corpus (Grust evidence
/// fable-c2-corrected-review: 68,588 strings, too large to embed). Set
/// SAIL_FABLE_LOWERCASE_CORPUS to a JSON `{cases: [{input, lower}]}`.
#[test]
fn optional_external_jvm_corpus_replays_against_the_helper() {
    #[derive(Deserialize)]
    struct Corpus {
        cases: Vec<External>,
    }
    #[derive(Deserialize)]
    struct External {
        input: String,
        lower: String,
    }
    let Ok(path) = std::env::var("SAIL_FABLE_LOWERCASE_CORPUS") else {
        eprintln!("SAIL_FABLE_LOWERCASE_CORPUS unset; external corpus replay skipped");
        return;
    };
    #[expect(clippy::expect_used)]
    let text = std::fs::read_to_string(&path).expect("the supplied JVM corpus should be readable");
    #[expect(clippy::expect_used)]
    let corpus = serde_json::from_str::<Corpus>(&text)
        .expect("the supplied JVM corpus should contain valid cases");
    let mut mismatches = 0;
    for case in &corpus.cases {
        if spark_root_lowercase(&case.input) != case.lower {
            mismatches += 1;
            if mismatches <= 5 {
                eprintln!("mismatch for {:?}", case.input);
            }
        }
    }
    eprintln!(
        "external corpus: {} strings, {mismatches} mismatches",
        corpus.cases.len()
    );
    assert_eq!(mismatches, 0);
    assert!(!corpus.cases.is_empty());
}

#[test]
fn lookup_lowercase_and_resolver_equality_are_different_contracts() {
    // Spark's case-insensitive column lookup compares ROOT-lowercased names;
    // its resolver compares with `equalsIgnoreCase`. They disagree here.
    for (left, right, lookup_equal, resolver_equal) in [
        ("İ", "i", false, true),
        ("I", "ı", false, true),
        ("Σ", "ς", false, true),
        ("ΟΔΟΣ", "οδοσ", false, true),
        ("ΟΔΟΣ", "οδος", true, true),
        ("\u{10570}", "\u{10597}", false, false),
        ("\u{1C89}", "\u{1C8A}", false, false),
        ("É", "é", true, true),
        ("Name", "NAME", true, true),
    ] {
        assert_eq!(
            spark_root_lowercase(left) == spark_root_lowercase(right),
            lookup_equal,
            "lookup equality of {left:?} and {right:?}"
        );
        assert_eq!(
            spark_case_insensitive_name_eq(left, right),
            resolver_equal,
            "resolver equality of {left:?} and {right:?}"
        );
    }
}
