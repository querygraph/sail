// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Rust translation of Apache Commons Codec 1.21.0 `DoubleMetaphone.java`.
//!
//! Source: https://github.com/apache/commons-codec/blob/rel/commons-codec-1.21.0/src/main/java/org/apache/commons/codec/language/DoubleMetaphone.java
//! Modifications: the input is already uppercase ASCII, and only the primary
//! four-character result is needed. Alternate-only appends are omitted; once
//! primary is full, further appends cannot change it, so encoding can stop.

struct Encoder<'a> {
    word: &'a [u8],
    output: String,
    slavo_germanic: bool,
}

pub(super) fn encode(word: &[u8]) -> String {
    let mut encoder = Encoder {
        word,
        output: String::with_capacity(4),
        slavo_germanic: word.contains(&b'W')
            || word.contains(&b'K')
            || word.windows(2).any(|part| part == b"CZ")
            || word.windows(4).any(|part| part == b"WITZ"),
    };
    let mut index = if encoder.contains(0, 2, &["GN", "KN", "PN", "WR", "PS"]) {
        1
    } else {
        0
    };
    while encoder.output.len() < 4 && index < encoder.len() {
        index = match encoder.at(index) {
            b'A' | b'E' | b'I' | b'O' | b'U' | b'Y' => {
                if index == 0 {
                    encoder.append("A");
                }
                index + 1
            }
            b'B' => encoder.doubled(index, b'B', "P"),
            b'C' => encoder.c(index),
            b'D' => encoder.d(index),
            b'F' => encoder.doubled(index, b'F', "F"),
            b'G' => encoder.g(index),
            b'H' => encoder.h(index),
            b'J' => encoder.j(index),
            b'K' => encoder.doubled(index, b'K', "K"),
            b'L' => encoder.doubled(index, b'L', "L"),
            b'M' => {
                encoder.append("M");
                if encoder.at(index + 1) == b'M'
                    || (encoder.contains(index - 1, 3, &["UMB"])
                        && (index + 1 == encoder.len() - 1
                            || encoder.contains(index + 2, 2, &["ER"])))
                {
                    index + 2
                } else {
                    index + 1
                }
            }
            b'N' => encoder.doubled(index, b'N', "N"),
            b'P' => encoder.p(index),
            b'Q' => encoder.doubled(index, b'Q', "K"),
            b'R' => encoder.r(index),
            b'S' => encoder.s(index),
            b'T' => encoder.t(index),
            b'V' => encoder.doubled(index, b'V', "F"),
            b'W' => encoder.w(index),
            b'X' => encoder.x(index),
            b'Z' => encoder.z(index),
            _ => index + 1,
        };
    }
    encoder.output
}

fn vowel(letter: u8) -> bool {
    b"AEIOUY".contains(&letter)
}

impl Encoder<'_> {
    fn len(&self) -> isize {
        self.word.len() as isize
    }

    fn at(&self, index: isize) -> u8 {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.word.get(index))
            .copied()
            .unwrap_or(0)
    }

    fn contains(&self, start: isize, length: usize, choices: &[&str]) -> bool {
        let Ok(start) = usize::try_from(start) else {
            return false;
        };
        let Some(end) = start.checked_add(length) else {
            return false;
        };
        self.word
            .get(start..end)
            .is_some_and(|part| choices.iter().any(|choice| part == choice.as_bytes()))
    }

    fn append(&mut self, code: &str) {
        let remaining = 4 - self.output.len();
        self.output.push_str(&code[..code.len().min(remaining)]);
    }

    fn doubled(&mut self, index: isize, letter: u8, code: &str) -> isize {
        self.append(code);
        index + if self.at(index + 1) == letter { 2 } else { 1 }
    }

    fn condition_c0(&self, i: isize) -> bool {
        if self.contains(i, 4, &["CHIA"]) {
            return true;
        }
        if i <= 1 || vowel(self.at(i - 2)) || !self.contains(i - 1, 3, &["ACH"]) {
            return false;
        }
        (self.at(i + 2) != b'I' && self.at(i + 2) != b'E')
            || self.contains(i - 2, 6, &["BACHER", "MACHER"])
    }

    fn condition_ch0(&self, i: isize) -> bool {
        i == 0
            && (self.contains(i + 1, 5, &["HARAC", "HARIS"])
                || self.contains(i + 1, 3, &["HOR", "HYM", "HIA", "HEM"]))
            && !self.contains(0, 5, &["CHORE"])
    }

    fn condition_ch1(&self, i: isize) -> bool {
        self.contains(0, 4, &["VAN ", "VON "])
            || self.contains(0, 3, &["SCH"])
            || self.contains(i - 2, 6, &["ORCHES", "ARCHIT", "ORCHID"])
            || self.contains(i + 2, 1, &["T", "S"])
            || ((self.contains(i - 1, 1, &["A", "O", "U", "E"]) || i == 0)
                && (self.contains(
                    i + 2,
                    1,
                    &["L", "R", "N", "M", "B", "H", "F", "V", "W", " "],
                ) || i + 1 == self.len() - 1))
    }

    fn c(&mut self, i: isize) -> isize {
        if self.condition_c0(i) {
            self.append("K");
            i + 2
        } else if i == 0 && self.contains(i, 6, &["CAESAR"]) {
            self.append("S");
            i + 2
        } else if self.contains(i, 2, &["CH"]) {
            self.ch(i)
        } else if self.contains(i, 2, &["CZ"]) && !self.contains(i - 2, 4, &["WICZ"]) {
            self.append("S");
            i + 2
        } else if self.contains(i + 1, 3, &["CIA"]) {
            self.append("X");
            i + 3
        } else if self.contains(i, 2, &["CC"]) && !(i == 1 && self.at(0) == b'M') {
            self.cc(i)
        } else if self.contains(i, 2, &["CK", "CG", "CQ"]) {
            self.append("K");
            i + 2
        } else if self.contains(i, 2, &["CI", "CE", "CY"]) {
            self.append("S");
            i + 2
        } else {
            self.append("K");
            if self.contains(i + 1, 2, &[" C", " Q", " G"]) {
                i + 3
            } else if self.contains(i + 1, 1, &["C", "K", "Q"])
                && !self.contains(i + 1, 2, &["CE", "CI"])
            {
                i + 2
            } else {
                i + 1
            }
        }
    }

    fn cc(&mut self, i: isize) -> isize {
        if self.contains(i + 2, 1, &["I", "E", "H"]) && !self.contains(i + 2, 2, &["HU"]) {
            if (i == 1 && self.at(i - 1) == b'A') || self.contains(i - 1, 5, &["UCCEE", "UCCES"]) {
                self.append("KS");
            } else {
                self.append("X");
            }
            i + 3
        } else {
            self.append("K");
            i + 2
        }
    }

    fn ch(&mut self, i: isize) -> isize {
        if (i > 0 && self.contains(i, 4, &["CHAE"]))
            || self.condition_ch0(i)
            || self.condition_ch1(i)
            || (i > 0 && self.contains(0, 2, &["MC"]))
        {
            self.append("K");
        } else {
            self.append("X");
        }
        i + 2
    }

    fn d(&mut self, i: isize) -> isize {
        if self.contains(i, 2, &["DG"]) {
            if self.contains(i + 2, 1, &["I", "E", "Y"]) {
                self.append("J");
                i + 3
            } else {
                self.append("TK");
                i + 2
            }
        } else {
            self.append("T");
            i + if self.contains(i, 2, &["DT", "DD"]) {
                2
            } else {
                1
            }
        }
    }

    fn g(&mut self, i: isize) -> isize {
        if self.at(i + 1) == b'H' {
            self.gh(i)
        } else if self.at(i + 1) == b'N' {
            if i == 1 && vowel(self.at(0)) && !self.slavo_germanic {
                self.append("KN");
            } else if !self.contains(i + 2, 2, &["EY"])
                && self.at(i + 1) != b'Y'
                && !self.slavo_germanic
            {
                self.append("N");
            } else {
                self.append("KN");
            }
            i + 2
        } else if self.contains(i + 1, 2, &["LI"]) && !self.slavo_germanic {
            self.append("KL");
            i + 2
        } else if (i == 0
            && (self.at(i + 1) == b'Y'
                || self.contains(
                    i + 1,
                    2,
                    &[
                        "ES", "EP", "EB", "EL", "EY", "IB", "IL", "IN", "IE", "EI", "ER",
                    ],
                )))
            || ((self.contains(i + 1, 2, &["ER"]) || self.at(i + 1) == b'Y')
                && !self.contains(0, 6, &["DANGER", "RANGER", "MANGER"])
                && !self.contains(i - 1, 1, &["E", "I"])
                && !self.contains(i - 1, 3, &["RGY", "OGY"]))
        {
            self.append("K");
            i + 2
        } else if self.contains(i + 1, 1, &["E", "I", "Y"])
            || self.contains(i - 1, 4, &["AGGI", "OGGI"])
        {
            if self.contains(0, 4, &["VAN ", "VON "])
                || self.contains(0, 3, &["SCH"])
                || self.contains(i + 1, 2, &["ET"])
            {
                self.append("K");
            } else {
                self.append("J");
            }
            i + 2
        } else {
            self.doubled(i, b'G', "K")
        }
    }

    fn gh(&mut self, i: isize) -> isize {
        if i > 0 && !vowel(self.at(i - 1)) {
            self.append("K");
        } else if i == 0 {
            self.append(if self.at(i + 2) == b'I' { "J" } else { "K" });
        } else if !((i > 1 && self.contains(i - 2, 1, &["B", "H", "D"]))
            || (i > 2 && self.contains(i - 3, 1, &["B", "H", "D"]))
            || (i > 3 && self.contains(i - 4, 1, &["B", "H"])))
        {
            if i > 2
                && self.at(i - 1) == b'U'
                && self.contains(i - 3, 1, &["C", "G", "L", "R", "T"])
            {
                self.append("F");
            } else if i > 0 && self.at(i - 1) != b'I' {
                self.append("K");
            }
        }
        i + 2
    }

    fn h(&mut self, i: isize) -> isize {
        if (i == 0 || vowel(self.at(i - 1))) && vowel(self.at(i + 1)) {
            self.append("H");
            i + 2
        } else {
            i + 1
        }
    }

    fn j(&mut self, i: isize) -> isize {
        if self.contains(i, 4, &["JOSE"]) || self.contains(0, 4, &["SAN "]) {
            if (i == 0 && self.at(i + 4) == b' ')
                || self.len() == 4
                || self.contains(0, 4, &["SAN "])
            {
                self.append("H");
            } else {
                self.append("J");
            }
            i + 1
        } else {
            if (i == 0 && !self.contains(i, 4, &["JOSE"]))
                || (vowel(self.at(i - 1))
                    && !self.slavo_germanic
                    && matches!(self.at(i + 1), b'A' | b'O'))
                || i == self.len() - 1
                || (!self.contains(i + 1, 1, &["L", "T", "K", "S", "N", "M", "B", "Z"])
                    && !self.contains(i - 1, 1, &["S", "K", "L"]))
            {
                self.append("J");
            }
            i + if self.at(i + 1) == b'J' { 2 } else { 1 }
        }
    }

    fn p(&mut self, i: isize) -> isize {
        if self.at(i + 1) == b'H' {
            self.append("F");
            i + 2
        } else {
            self.append("P");
            i + if self.contains(i + 1, 1, &["P", "B"]) {
                2
            } else {
                1
            }
        }
    }

    fn r(&mut self, i: isize) -> isize {
        if !(i == self.len() - 1
            && !self.slavo_germanic
            && self.contains(i - 2, 2, &["IE"])
            && !self.contains(i - 4, 2, &["ME", "MA"]))
        {
            self.append("R");
        }
        i + if self.at(i + 1) == b'R' { 2 } else { 1 }
    }

    fn s(&mut self, i: isize) -> isize {
        if self.contains(i - 1, 3, &["ISL", "YSL"]) {
            i + 1
        } else if i == 0 && self.contains(i, 5, &["SUGAR"]) {
            self.append("X");
            i + 1
        } else if self.contains(i, 2, &["SH"]) {
            self.append(
                if self.contains(i + 1, 4, &["HEIM", "HOEK", "HOLM", "HOLZ"]) {
                    "S"
                } else {
                    "X"
                },
            );
            i + 2
        } else if self.contains(i, 3, &["SIO", "SIA"]) || self.contains(i, 4, &["SIAN"]) {
            self.append("S");
            i + 3
        } else if (i == 0 && self.contains(i + 1, 1, &["M", "N", "L", "W"]))
            || self.contains(i + 1, 1, &["Z"])
        {
            self.append("S");
            i + if self.contains(i + 1, 1, &["Z"]) {
                2
            } else {
                1
            }
        } else if self.contains(i, 2, &["SC"]) {
            self.sc(i)
        } else {
            if !(i == self.len() - 1 && self.contains(i - 2, 2, &["AI", "OI"])) {
                self.append("S");
            }
            i + if self.contains(i + 1, 1, &["S", "Z"]) {
                2
            } else {
                1
            }
        }
    }

    fn sc(&mut self, i: isize) -> isize {
        if self.at(i + 2) == b'H' {
            if self.contains(i + 3, 2, &["OO", "ER", "EN", "UY", "ED", "EM"]) {
                self.append(if self.contains(i + 3, 2, &["ER", "EN"]) {
                    "X"
                } else {
                    "SK"
                });
            } else {
                self.append("X");
            }
        } else if self.contains(i + 2, 1, &["I", "E", "Y"]) {
            self.append("S");
        } else {
            self.append("SK");
        }
        i + 3
    }

    fn t(&mut self, i: isize) -> isize {
        if self.contains(i, 4, &["TION"]) || self.contains(i, 3, &["TIA", "TCH"]) {
            self.append("X");
            i + 3
        } else if self.contains(i, 2, &["TH"]) || self.contains(i, 3, &["TTH"]) {
            self.append(
                if self.contains(i + 2, 2, &["OM", "AM"])
                    || self.contains(0, 4, &["VAN ", "VON "])
                    || self.contains(0, 3, &["SCH"])
                {
                    "T"
                } else {
                    "0"
                },
            );
            i + 2
        } else {
            self.append("T");
            i + if self.contains(i + 1, 1, &["T", "D"]) {
                2
            } else {
                1
            }
        }
    }

    fn w(&mut self, i: isize) -> isize {
        if self.contains(i, 2, &["WR"]) {
            self.append("R");
            i + 2
        } else if i == 0 && (vowel(self.at(i + 1)) || self.contains(i, 2, &["WH"])) {
            self.append("A");
            i + 1
        } else if (i == self.len() - 1 && vowel(self.at(i - 1)))
            || self.contains(i - 1, 5, &["EWSKI", "EWSKY", "OWSKI", "OWSKY"])
            || self.contains(0, 3, &["SCH"])
        {
            i + 1
        } else if self.contains(i, 4, &["WICZ", "WITZ"]) {
            self.append("TS");
            i + 4
        } else {
            i + 1
        }
    }

    fn x(&mut self, i: isize) -> isize {
        if i == 0 {
            self.append("S");
            i + 1
        } else {
            if !(i == self.len() - 1
                && (self.contains(i - 3, 3, &["IAU", "EAU"])
                    || self.contains(i - 2, 2, &["AU", "OU"])))
            {
                self.append("KS");
            }
            i + if self.contains(i + 1, 1, &["C", "X"]) {
                2
            } else {
                1
            }
        }
    }

    fn z(&mut self, i: isize) -> isize {
        if self.at(i + 1) == b'H' {
            self.append("J");
            i + 2
        } else {
            self.doubled(i, b'Z', "S")
        }
    }
}
