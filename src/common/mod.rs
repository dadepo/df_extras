use anyhow::anyhow;
use serde_json_path::JsonPath;

pub mod test_utils;

pub(crate) fn get_value_at(
    json: serde_json::Value,
    path: &str,
) -> anyhow::Result<serde_json::Value> {
    let path = JsonPath::parse(path)?;
    path.query(&json)
        .exactly_one().cloned()
        .map_err(|err| anyhow!(err))
}

pub(crate) fn get_value_at_string(
    json_string: &str,
    path: &str,
) -> anyhow::Result<serde_json::Value> {
    let json_value: serde_json::Value = serde_json::from_str(json_string)?;
    get_value_at(json_value, path)
}

pub(crate) fn get_json_type(json_value: &serde_json::Value) -> anyhow::Result<String> {
    let result = if json_value.is_boolean() {
        match json_value.as_bool() {
            Some(bool_value) => bool_value.to_string(),
            None => return Err(anyhow!("Internal error".to_string())),
        }
    } else if json_value.is_number() {
        if json_value.to_string().parse::<i64>().is_ok() {
            "integer".to_string()
        } else {
            "real".to_string()
        }
    } else if json_value.is_f64() || json_value.is_i64() || json_value.is_u64() {
        "real".to_string()
    } else if json_value.is_string() {
        "text".to_string()
    } else if json_value.is_array() {
        "array".to_string()
    } else if json_value.is_object() {
        "object".to_string()
    } else if json_value.is_null() {
        "null".to_string()
    } else {
        return Err(anyhow!("Unknown json type".to_string()));
    };

    Ok(result)
}

pub(crate) fn get_json_string_type(json_string: &str) -> anyhow::Result<String> {
    let json_value: serde_json::Value =
        serde_json::from_str(json_string).map_err(|err| anyhow!(err))?;
    get_json_type(&json_value)
}

#[cfg(test)]
mod test {
    use crate::common::get_value_at;
    use serde_json::json;

    #[test]
    fn test_get_value_at() {
        let value = json!({"foo": ["bar", "baz"]});
        let result = get_value_at(value, "$.foo").unwrap();
        assert_eq!(result, json!(["bar", "baz"]));
    }
}
