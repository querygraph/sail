//! Java BigDecimal's string grammar and Spark's bounded HALF_UP rescaling.
//! Work and storage depend on the input length and target precision, never on
//! the magnitude of the exponent. Java parses UTF-16 chars, not code points.

#[derive(Debug)]
pub(super) struct Decimal {
    negative: bool,
    digits: Vec<u8>,
    scale: i32,
}

#[derive(Debug)]
pub(super) enum CastError {
    Invalid,
    UnsupportedRange,
    Overflow(Decimal),
}

// Character.digit(char, 10): these BMP blocks each contain exactly 0..9.
// Verified exhaustively against JDK20.0.2's 65536 char values. Supplementary
// digits are surrogate pairs in Java and are not accepted by BigDecimal.
const DIGIT_STARTS: [u32; 37] = [
    48, 1632, 1776, 1984, 2406, 2534, 2662, 2790, 2918, 3046, 3174, 3302, 3430, 3558, 3664, 3792,
    3872, 4160, 4240, 6112, 6160, 6470, 6608, 6784, 6800, 6992, 7088, 7232, 7248, 42528, 43216,
    43264, 43472, 43504, 43600, 44016, 65296,
];

fn digit(c: char) -> Option<u8> {
    if c.is_ascii_digit() {
        return Some(c as u8 - b'0');
    }
    let code = u32::from(c);
    DIGIT_STARTS
        .iter()
        .find_map(|&start| code.checked_sub(start).filter(|&d| d < 10).map(|d| d as u8))
}

impl Decimal {
    fn parse(input: &str) -> Result<Self, CastError> {
        let mut chars = input.trim_matches(|c| c <= '\u{20}').chars().peekable();
        let negative = match chars.peek() {
            Some('-') => {
                chars.next();
                true
            }
            Some('+') => {
                chars.next();
                false
            }
            _ => false,
        };
        let mut digits = Vec::new();
        let mut point = false;
        let mut fraction = 0_i64;
        while let Some(&c) = chars.peek() {
            if let Some(d) = digit(c) {
                digits.push(d);
                if point {
                    fraction += 1;
                }
            } else if c == '.' && !point {
                point = true;
            } else {
                break;
            }
            chars.next();
        }
        if digits.is_empty() {
            return Err(CastError::Invalid);
        }
        let mut exponent = 0_i64;
        if matches!(chars.peek(), Some('e' | 'E')) {
            chars.next();
            let negative_exponent = match chars.peek() {
                Some('-') => {
                    chars.next();
                    true
                }
                Some('+') => {
                    chars.next();
                    false
                }
                _ => false,
            };
            let mut any = false;
            let mut significant = 0;
            for c in chars.by_ref() {
                let d = digit(c).ok_or(CastError::Invalid)?;
                any = true;
                if d != 0 || significant != 0 {
                    significant += 1;
                }
                // BigDecimal.parseExp permits ten significant exponent digits.
                if significant > 10 {
                    return Err(CastError::Invalid);
                }
                exponent = exponent * 10 + i64::from(d);
            }
            if !any {
                return Err(CastError::Invalid);
            }
            if negative_exponent {
                exponent = -exponent;
            }
        }
        if chars.next().is_some() {
            return Err(CastError::Invalid);
        }
        let scale = fraction
            .checked_sub(exponent)
            .and_then(|s| i32::try_from(s).ok())
            .ok_or(CastError::Invalid)?;
        let leading = digits.iter().position(|&d| d != 0).unwrap_or(digits.len());
        digits.drain(..leading);
        Ok(Self {
            negative,
            digits,
            scale,
        })
    }

    pub(super) fn error_value(&self) -> String {
        let digits: String = self.digits.iter().map(|d| char::from(b'0' + d)).collect();
        let sign = if self.negative { "-" } else { "" };
        // Spark DataTypeErrors uses plain notation except below scale -1000.
        if self.scale < -1000 {
            let exponent = self.digits.len() as i64 - 1 - i64::from(self.scale);
            let (first, rest) = digits.split_at(1);
            if rest.is_empty() {
                format!("{sign}{first}E+{exponent}")
            } else {
                format!("{sign}{first}.{rest}E+{exponent}")
            }
        } else if self.scale <= 0 {
            format!(
                "{sign}{digits}{}",
                "0".repeat((-i64::from(self.scale)) as usize)
            )
        } else {
            let scale = self.scale as usize;
            if scale >= digits.len() {
                format!("{sign}0.{}{digits}", "0".repeat(scale - digits.len()))
            } else {
                let (whole, fraction) = digits.split_at(digits.len() - scale);
                format!("{sign}{whole}.{fraction}")
            }
        }
    }
}

pub(super) fn cast(
    input: &str,
    precision: u8,
    scale: i8,
    allow_negative_scale: bool,
) -> Result<i128, CastError> {
    let decimal = Decimal::parse(input)?;
    if decimal.digits.is_empty() {
        return Ok(0);
    }
    if !allow_negative_scale && decimal.digits.len() as i128 - i128::from(decimal.scale) > 38 {
        return Err(CastError::UnsupportedRange);
    }
    let kept = decimal.digits.len() as i128 + i128::from(scale) - i128::from(decimal.scale);
    if kept < 0 {
        return Ok(0);
    }
    if kept > i128::from(precision) {
        return Err(CastError::Overflow(decimal));
    }
    let kept = kept as usize;
    let mut value = 0_i128;
    for index in 0..kept {
        value = value * 10 + i128::from(decimal.digits.get(index).copied().unwrap_or(0));
    }
    if decimal.digits.get(kept).is_some_and(|&d| d >= 5) {
        value += 1;
    }
    if value >= 10_i128.pow(u32::from(precision)) {
        return Err(CastError::Overflow(decimal));
    }
    Ok(if decimal.negative { -value } else { value })
}
