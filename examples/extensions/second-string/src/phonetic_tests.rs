use super::{double_metaphone, normalize, refined_soundex, soundex};

#[test]
fn soundex_matches_reference_scala_examples() {
    for (word, code) in [
        ("Robert", "R163"),
        ("Rupert", "R163"),
        ("Ashcraft", "A261"),
        ("Tymczak", "T522"),
        ("Pfister", "P236"),
        ("A", "A000"),
        ("123", ""),
    ] {
        assert_eq!(soundex(word), code, "{word}");
    }
}

#[test]
fn soundex_vowels_reset_duplicates_but_h_and_w_do_not() {
    assert_eq!(soundex("BHB"), "B000");
    assert_eq!(soundex("BWB"), "B000");
    for vowel in ['A', 'E', 'I', 'O', 'U', 'Y'] {
        assert_eq!(soundex(&format!("B{vowel}B")), "B100");
    }
    // Removed punctuation does not become a separator in the encoder.
    assert_eq!(soundex("B-B"), "B000");
}

#[test]
fn refined_soundex_uses_wrapper_initial_code_and_zero_semantics() {
    // The Scala wrapper omits the initial letter's digit, unlike Commons
    // RefinedSoundex, and includes subsequent zero-code transitions.
    assert_eq!(refined_soundex("Robert"), "R01093");
    assert_eq!(refined_soundex("Rupert"), "R01093");
    assert_eq!(refined_soundex("Smith"), "S8030");
    assert_eq!(refined_soundex("smith"), "S8030");
    assert_eq!(refined_soundex("A"), "A");
    assert_eq!(refined_soundex("BHB"), "B01");
    assert_eq!(refined_soundex("AAA"), "A");
    assert_eq!(refined_soundex("123"), "");
}

#[test]
fn normalization_follows_java_simple_uppercase_of_utf16_units() {
    assert_eq!(normalize("aBzıſ"), b"ABZIS");
    assert_eq!(normalize("İéñçßﬀKＫ𝕂𐐨😀"), b"");
    assert_eq!(normalize("Émile O'Brien-12"), b"MILEOBRIEN");
    assert_eq!(normalize("e\u{0301}"), b"E");
    for encode in [soundex, refined_soundex, double_metaphone] {
        assert_eq!(encode("ıſ"), encode("IS"));
        assert_eq!(encode("ßﬀ𝕂"), "");
        assert_eq!(encode("Émile"), encode("mile"));
        assert_eq!(encode("É"), "");
        assert_eq!(encode("O'Brien"), encode("OBRIEN"));
        assert_eq!(encode("San Jose"), encode("SANJOSE"));
        assert_eq!(encode("a\0b"), encode("AB"));
        assert_eq!(encode(""), "");
    }
}

#[test]
fn double_metaphone_matches_commons_121_primary_examples() {
    // Alphabetic examples from Apache Commons Codec 1.21.0's test suite.
    for (word, code) in [
        ("testing", "TSTN"),
        ("The", "0"),
        ("quick", "KK"),
        ("brown", "PRN"),
        ("fox", "FKS"),
        ("jumped", "JMPT"),
        ("over", "AFR"),
        ("lazy", "LS"),
        ("dogs", "TKS"),
        ("MacCafferey", "MKFR"),
        ("Stephan", "STFN"),
        ("Kuczewski", "KSSK"),
        ("McClelland", "MKLL"),
        ("xenophobia", "SNFP"),
    ] {
        assert_eq!(double_metaphone(word), code, "{word}");
        assert_eq!(double_metaphone(&word.to_uppercase()), code, "{word}");
    }
    assert_eq!(double_metaphone("Stephen"), double_metaphone("Steven"));
}

#[test]
fn double_metaphone_covers_silent_initials_and_truncation() {
    for (word, code) in [
        ("gnome", "NM"),
        ("knight", "NT"),
        ("pneumonia", "NMN"),
        ("write", "RT"),
        ("psalm", "SLM"),
    ] {
        assert_eq!(double_metaphone(word), code, "{word}");
    }
    assert_eq!(double_metaphone("Ababababab"), "APPP");
    assert_eq!(double_metaphone("123"), "");
}
