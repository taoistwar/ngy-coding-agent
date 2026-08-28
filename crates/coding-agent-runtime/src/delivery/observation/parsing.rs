use super::DeliverySourceError;

const MAX_ATTRIBUTE_INPUT_BYTES: usize = 64 * 1024;

pub(crate) fn parse_object_id(
    output: &[u8],
    hexadecimal_length: usize,
) -> Result<&str, DeliverySourceError> {
    let value = parse_one_line(output)?;
    if value.len() == hexadecimal_length
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(value)
    } else {
        Err(DeliverySourceError::AuthenticationChanged)
    }
}

pub(super) fn parse_one_line(output: &[u8]) -> Result<&str, DeliverySourceError> {
    let mut value = output;
    if value.last() == Some(&b'\n') {
        value = &value[..value.len() - 1];
        if value.last() == Some(&b'\r') {
            value = &value[..value.len() - 1];
        }
    }
    if value.is_empty() || value.contains(&0) || value.contains(&b'\n') || value.contains(&b'\r') {
        return Err(DeliverySourceError::AuthenticationChanged);
    }
    std::str::from_utf8(value).map_err(|_| DeliverySourceError::AuthenticationChanged)
}

pub(super) fn attribute_path_chunks(
    paths: &[Vec<u8>],
) -> Result<Vec<&[Vec<u8>]>, DeliverySourceError> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (index, path) in paths.iter().enumerate() {
        let framed = path
            .len()
            .checked_add(1)
            .ok_or(DeliverySourceError::BoundsExceeded)?;
        if framed > MAX_ATTRIBUTE_INPUT_BYTES {
            return Err(DeliverySourceError::BoundsExceeded);
        }
        if bytes != 0 && bytes.saturating_add(framed) > MAX_ATTRIBUTE_INPUT_BYTES {
            chunks.push(&paths[start..index]);
            start = index;
            bytes = 0;
        }
        bytes = bytes
            .checked_add(framed)
            .ok_or(DeliverySourceError::BoundsExceeded)?;
    }
    if start < paths.len() {
        chunks.push(&paths[start..]);
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_line_parser_rejects_extra_or_non_utf8_data() {
        assert_eq!(parse_one_line(b"value\n").unwrap(), "value");
        assert!(parse_one_line(b"one\ntwo\n").is_err());
        assert!(parse_one_line(b"bad\xff\n").is_err());
    }

    #[test]
    fn object_id_parser_requires_the_probed_object_format_length() {
        let sha1 = format!("{}\n", "a".repeat(40));
        let sha256 = format!("{}\n", "b".repeat(64));
        assert!(parse_object_id(sha1.as_bytes(), 40).is_ok());
        assert!(parse_object_id(sha256.as_bytes(), 64).is_ok());
        assert!(parse_object_id(sha1.as_bytes(), 64).is_err());
        assert!(parse_object_id(sha256.as_bytes(), 40).is_err());
    }

    #[test]
    fn attribute_chunks_never_exceed_exact_input_budget() {
        let paths = vec![vec![b'a'; 40_000], vec![b'b'; 40_000]];
        let chunks = attribute_path_chunks(&paths).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], &paths[0..1]);
        assert_eq!(chunks[1], &paths[1..2]);
    }
}
