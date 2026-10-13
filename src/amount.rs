use anyhow::{Result, ensure};

pub(crate) const ZATOSHIS_PER_TAZ: i64 = 100_000_000;
pub(crate) const MAX_AMOUNT_ZATOSHIS: i64 = 2_100_000_000_000_000;

pub(crate) fn parse_taz(value: &str) -> Result<i64> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    ensure!(
        !whole.is_empty()
            && (!value.contains('.') || !fraction.is_empty())
            && whole.bytes().all(|byte| byte.is_ascii_digit())
            && fraction.bytes().all(|byte| byte.is_ascii_digit())
            && fraction.len() <= 8,
        "amount must be a positive decimal TAZ amount with at most 8 decimal places"
    );
    let whole = whole.parse::<i64>()?;
    let fractional_zatoshis = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>()? * 10_i64.pow((8 - fraction.len()) as u32)
    };
    let amount = whole
        .checked_mul(ZATOSHIS_PER_TAZ)
        .and_then(|value| value.checked_add(fractional_zatoshis))
        .ok_or_else(|| anyhow::anyhow!("amount is out of range"))?;
    validate_zatoshis(amount)?;
    Ok(amount)
}

pub(crate) fn validate_zatoshis(amount: i64) -> Result<()> {
    ensure!(
        (1..=MAX_AMOUNT_ZATOSHIS).contains(&amount),
        "amount must be greater than 0 and no more than 21,000,000 TAZ"
    );
    Ok(())
}

pub(crate) fn format_taz(amount: i64) -> String {
    let whole = amount / ZATOSHIS_PER_TAZ;
    let fraction = amount % ZATOSHIS_PER_TAZ;
    if fraction == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{fraction:08}")
            .trim_end_matches('0')
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{format_taz, parse_taz};

    #[test]
    fn parses_and_formats_exact_taz_amounts() {
        for (text, zatoshis) in [
            ("1", 100_000_000),
            ("0.1", 10_000_000),
            ("0.00000001", 1),
            ("12.34005000", 1_234_005_000),
        ] {
            assert_eq!(parse_taz(text).unwrap(), zatoshis);
            assert_eq!(
                format_taz(zatoshis),
                text.trim_end_matches('0').trim_end_matches('.')
            );
        }
    }

    #[test]
    fn rejects_zero_precision_overflow_and_non_decimal_input() {
        for value in [
            "0",
            "0.00000000",
            "-0.1",
            "+0.1",
            ".1",
            "1.",
            "1e-2",
            "0.000000001",
        ] {
            assert!(parse_taz(value).is_err(), "{value}");
        }
        assert!(parse_taz("21000000.00000001").is_err());
    }
}
