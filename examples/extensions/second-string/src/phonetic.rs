//! Native equivalents of spark-second-string's unary phonetic wrappers.
//!
//! These wrappers normalize before encoding, including before Commons Codec's
//! Double Metaphone. Their Soundex and Refined Soundex are the Scala project's
//! implementations, not Apache Commons Codec's similarly named encoders.

#[path = "double_metaphone.rs"]
mod double_metaphone_impl;

/// Java `Character.toUpperCase(char)` followed by the wrapper's A–Z filter.
///
/// Java visits UTF-16 code units and uses a one-character uppercase mapping:
/// neither supplementary letters nor uppercase expansions such as ß → SS
/// survive. The two non-ASCII code units whose uppercase is ASCII are ı and ſ.
fn normalize(input: &str) -> Vec<u8> {
    input
        .chars()
        .filter_map(|c| match c {
            'A'..='Z' => Some(c as u8),
            'a'..='z' => Some(c as u8 - b'a' + b'A'),
            '\u{0131}' => Some(b'I'),
            '\u{017f}' => Some(b'S'),
            _ => None,
        })
        .collect()
}

/// Soundex as implemented by the reference Scala wrapper.
pub fn soundex(input: &str) -> String {
    const CODES: &[u8; 26] = b"01230120022455012623010202";
    let word = normalize(input);
    let Some(&first) = word.first() else {
        return String::new();
    };
    let mut output = String::with_capacity(4);
    output.push(char::from(first));
    let mut previous = CODES[usize::from(first - b'A')];
    for &letter in &word[1..] {
        let code = CODES[usize::from(letter - b'A')];
        if code != b'0' && code != previous {
            output.push(char::from(code));
            if output.len() == 4 {
                break;
            }
        }
        if code != b'0' {
            previous = code;
        } else if b"AEIOUY".contains(&letter) {
            previous = b'0';
        }
    }
    while output.len() < 4 {
        output.push('0');
    }
    output
}

/// Variable-length Refined Soundex as implemented by the Scala wrapper.
pub fn refined_soundex(input: &str) -> String {
    const CODES: &[u8; 26] = b"01230560062788012923050202";
    let word = normalize(input);
    let Some(&first) = word.first() else {
        return String::new();
    };
    let mut output = String::with_capacity(word.len() + 1);
    output.push(char::from(first));
    let mut previous = CODES[usize::from(first - b'A')];
    for &letter in &word[1..] {
        let code = CODES[usize::from(letter - b'A')];
        if code != previous {
            output.push(char::from(code));
        }
        previous = code;
    }
    output
}

/// Commons Codec 1.21.0 Double Metaphone primary code, capped at four bytes,
/// after the reference Scala wrapper's normalization.
pub fn double_metaphone(input: &str) -> String {
    double_metaphone_impl::encode(&normalize(input))
}

#[cfg(test)]
#[path = "phonetic_tests.rs"]
mod tests;
