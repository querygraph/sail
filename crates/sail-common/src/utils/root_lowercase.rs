//! `String.toLowerCase(Locale.ROOT)` as OpenJDK 17 computes it, for Spark's
//! `CaseInsensitiveMap` column lookup. This is a different contract from
//! `String.equalsIgnoreCase` ([`super::string::spark_case_insensitive_name_eq`]):
//! `İ` lowercases to two code points, a final capital sigma becomes `ς`, and
//! Java's simple mappings stop at Unicode 13.
//!
//! The scalar mapping is ICU's simple lowercase gated to Unicode 13
//! characters, verified exhaustively against the pinned JVM by hash. The
//! final-sigma rule is `ConditionalSpecialCasing.isFinalCased`: within the
//! word around the sigma, a cased letter before it and none after it, where
//! the word boundaries come from the JDK's own `RuleBasedBreakIterator`. That
//! iterator is ported below over UTF-16 units, including its lookahead and
//! backward tables, its surrogate stepping and its last-known-break cache;
//! its compiled tables live in `root_lowercase_tables.rs`, read from the JVM.
//! `root_lowercase_oracle.json` holds the JVM's answers the tests replay.

use std::sync::LazyLock;

use icu_casemap::CaseMapper;
use regex::Regex;

#[path = "root_lowercase_tables.rs"]
mod tables;

// OpenJDK 17 uses Unicode 13, so newer characters keep identity mappings.
#[expect(clippy::expect_used)]
static JDK_17_ASSIGNED_CHARACTER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\p{Age:13.0}$").expect("JDK 17 Unicode age pattern should be valid")
});

const GREEK_CAPITAL_SIGMA: u32 = 0x03A3;
const LATIN_CAPITAL_I_WITH_DOT_ABOVE: u32 = 0x0130;

/// Lowercases `name` exactly as `name.toLowerCase(Locale.ROOT)` does on OpenJDK 17.
pub fn spark_root_lowercase(name: &str) -> String {
    let case_mapper = CaseMapper::new();
    let units: Vec<u16> = name.encode_utf16().collect();
    let mut output = String::with_capacity(name.len());
    let mut index = 0;
    while index < units.len() {
        let code_point = code_point_at(&units, index);
        match code_point {
            GREEK_CAPITAL_SIGMA => output.push(if is_final_cased(&units, index) {
                'ς'
            } else {
                'σ'
            }),
            LATIN_CAPITAL_I_WITH_DOT_ABOVE => output.push_str("i\u{307}"),
            _ => {
                // `name` is valid UTF-8, so every code point is a scalar.
                if let Some(char) = char::from_u32(code_point) {
                    let mut buffer = [0; 4];
                    if JDK_17_ASSIGNED_CHARACTER.is_match(char.encode_utf8(&mut buffer)) {
                        output.push(case_mapper.simple_lowercase(char));
                    } else {
                        output.push(char);
                    }
                }
            }
        }
        index += char_count(code_point);
    }
    output
}

/// `ConditionalSpecialCasing.isCased` on this JDK.
fn is_cased(code_point: u32) -> bool {
    tables::CASED_RANGES
        .binary_search_by(|&(start, end)| range_order(start, end, code_point))
        .is_ok()
}

fn range_order(start: u32, end: u32, code_point: u32) -> std::cmp::Ordering {
    if code_point < start {
        std::cmp::Ordering::Greater
    } else if code_point > end {
        std::cmp::Ordering::Less
    } else {
        std::cmp::Ordering::Equal
    }
}

/// `ConditionalSpecialCasing.isFinalCased(src, index, Locale.ROOT)`, with a
/// fresh word iterator per call as the JDK creates one.
fn is_final_cased(units: &[u16], index: usize) -> bool {
    let mut word_boundary = WordBreaker::new(units);
    let mut i = index;
    loop {
        if word_boundary.is_boundary(i) {
            return false;
        }
        let char = code_point_before(units, i);
        if is_cased(char) {
            let mut after = index + char_count(code_point_at(units, index));
            while after < units.len() && !word_boundary.is_boundary(after) {
                let char = code_point_at(units, after);
                if is_cased(char) {
                    return false;
                }
                after += char_count(char);
            }
            return true;
        }
        let Some(previous) = i.checked_sub(char_count(char)) else {
            return false;
        };
        i = previous;
    }
}

fn is_high_surrogate(unit: u32) -> bool {
    (0xD800..=0xDBFF).contains(&unit)
}

fn is_low_surrogate(unit: u32) -> bool {
    (0xDC00..=0xDFFF).contains(&unit)
}

fn to_code_point(high: u32, low: u32) -> u32 {
    0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
}

fn char_count(code_point: u32) -> usize {
    if code_point >= 0x10000 { 2 } else { 1 }
}

/// `String.codePointAt(index)`.
fn code_point_at(units: &[u16], index: usize) -> u32 {
    let first = u32::from(units[index]);
    if is_high_surrogate(first)
        && let Some(&second) = units.get(index + 1)
        && is_low_surrogate(u32::from(second))
    {
        return to_code_point(first, u32::from(second));
    }
    first
}

/// `String.codePointBefore(index)`; `index` is at least one.
fn code_point_before(units: &[u16], index: usize) -> u32 {
    let second = u32::from(units[index - 1]);
    if is_low_surrogate(second)
        && index >= 2
        && let first = u32::from(units[index - 2])
        && is_high_surrogate(first)
    {
        return to_code_point(first, second);
    }
    second
}

/// `CharacterIterator.DONE`, which the JDK iterator also treats as a real
/// U+FFFF character would be treated: it ends a scan.
const CHAR_DONE: u32 = 0xFFFF;
/// `BreakIterator.DONE`.
const DONE: i64 = -1;
const START_STATE: usize = 1;
const STOP_STATE: usize = 0;
const IGNORE: i8 = -1;

/// `sun.text.RuleBasedBreakIterator` over a `StringCharacterIterator`,
/// restricted to what `isBoundary` needs.
struct WordBreaker<'a> {
    units: &'a [u16],
    position: usize,
    cached_last_known_break: i64,
}

impl<'a> WordBreaker<'a> {
    fn new(units: &'a [u16]) -> Self {
        Self {
            units,
            position: 0,
            cached_last_known_break: DONE,
        }
    }

    fn end(&self) -> usize {
        self.units.len()
    }

    // `StringCharacterIterator.current`, `next` and `previous`.
    fn unit_current(&self) -> u32 {
        self.units
            .get(self.position)
            .map_or(CHAR_DONE, |&unit| u32::from(unit))
    }

    fn unit_next(&mut self) -> u32 {
        if self.position + 1 < self.end() {
            self.position += 1;
            u32::from(self.units[self.position])
        } else {
            self.position = self.end();
            CHAR_DONE
        }
    }

    fn unit_previous(&mut self) -> u32 {
        if self.position > 0 {
            self.position -= 1;
            u32::from(self.units[self.position])
        } else {
            CHAR_DONE
        }
    }

    /// `getCurrent`.
    fn current(&mut self) -> u32 {
        let first = self.unit_current();
        if is_high_surrogate(first) && self.position < self.end() {
            let second = self.unit_next();
            self.unit_previous();
            if is_low_surrogate(second) {
                return to_code_point(first, second);
            }
        }
        first
    }

    /// `getCurrentCodePointCount`.
    fn current_count(&mut self) -> usize {
        let first = self.unit_current();
        if is_high_surrogate(first) && self.position < self.end() {
            let second = self.unit_next();
            self.unit_previous();
            if is_low_surrogate(second) {
                return 2;
            }
        }
        1
    }

    /// `getNext`: advances one code point, or returns `DONE` without moving
    /// when that would reach the end.
    fn next(&mut self) -> u32 {
        let index = self.position;
        if index == self.end() {
            return CHAR_DONE;
        }
        let advanced = index + self.current_count();
        if advanced >= self.end() {
            return CHAR_DONE;
        }
        self.position = advanced;
        self.current()
    }

    /// `getNextIndex`.
    fn next_index(&mut self) -> usize {
        (self.position + self.current_count()).min(self.end())
    }

    /// `getPrevious`.
    fn previous(&mut self) -> u32 {
        let second = self.unit_previous();
        if is_low_surrogate(second) && self.position > 0 {
            let first = self.unit_previous();
            if is_high_surrogate(first) {
                return to_code_point(first, second);
            }
            self.unit_next();
        }
        second
    }

    fn lookup_category(code_point: u32) -> i8 {
        tables::CATEGORY_RANGES
            .binary_search_by(|&(start, end, _)| range_order(start, end, code_point))
            .ok()
            .map_or(IGNORE, |found| tables::CATEGORY_RANGES[found].2)
    }

    fn lookup_state(table: &[i16], state: usize, category: i8) -> usize {
        let column = usize::try_from(category).unwrap_or(0);
        usize::try_from(table[state * tables::NUM_CATEGORIES + column]).unwrap_or(STOP_STATE)
    }

    /// `handleNext`.
    fn handle_next(&mut self) -> i64 {
        if self.position == self.end() {
            return DONE;
        }
        let mut result = self.next_index();
        let mut lookahead_result = 0;
        let mut state = START_STATE;
        let mut char = self.current();
        while char != CHAR_DONE && state != STOP_STATE {
            let category = Self::lookup_category(char);
            if category != IGNORE {
                state = Self::lookup_state(tables::STATE_TABLE, state, category);
            }
            if tables::LOOKAHEAD_STATES[state] {
                if tables::END_STATES[state] {
                    result = lookahead_result;
                } else {
                    lookahead_result = self.next_index();
                }
            } else if tables::END_STATES[state] {
                result = self.next_index();
            }
            char = self.next();
        }
        if char == CHAR_DONE && lookahead_result == self.end() {
            result = lookahead_result;
        }
        self.position = result;
        i64::try_from(result).unwrap_or(DONE)
    }

    /// `handlePrevious`.
    fn handle_previous(&mut self) -> i64 {
        let mut state = START_STATE;
        let mut category: i8 = 0;
        let mut last_category: i8 = 0;
        let mut char = self.current();
        while char != CHAR_DONE && state != STOP_STATE {
            last_category = category;
            category = Self::lookup_category(char);
            if category != IGNORE {
                state = Self::lookup_state(tables::BACKWARDS_STATE_TABLE, state, category);
            }
            char = self.previous();
        }
        if char != CHAR_DONE {
            if last_category != IGNORE {
                self.next();
                self.next();
            } else {
                self.next();
            }
        }
        i64::try_from(self.position).unwrap_or(DONE)
    }

    /// `following`.
    fn following(&mut self, offset: usize) -> i64 {
        self.position = offset;
        if offset == 0 {
            self.cached_last_known_break = self.handle_next();
            return self.cached_last_known_break;
        }
        let offset_i64 = i64::try_from(offset).unwrap_or(i64::MAX);
        let mut result = self.cached_last_known_break;
        if result >= offset_i64 || result <= DONE {
            result = self.handle_previous();
        } else {
            self.position = usize::try_from(result).unwrap_or(0);
        }
        while result != DONE && result <= offset_i64 {
            result = self.handle_next();
        }
        self.cached_last_known_break = result;
        result
    }

    /// `isBoundary`.
    fn is_boundary(&mut self, offset: usize) -> bool {
        offset == 0 || self.following(offset - 1) == i64::try_from(offset).unwrap_or(DONE)
    }
}

#[cfg(test)]
#[path = "root_lowercase_tests.rs"]
mod tests;
