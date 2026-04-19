use serde_json::Value;

pub(super) use polybet_manager_input::{value_as_bool_opt, value_as_f64_opt, value_as_string_opt};

pub(super) fn extract_first_market_item(body: &Value) -> Option<Value> {
    if let Some(arr) = body.as_array() {
        return arr.first().cloned();
    }
    let obj = body.as_object()?;
    for key in ["markets", "data", "items", "results"] {
        if let Some(value) = obj.get(key) {
            if let Some(arr) = value.as_array() {
                return arr.first().cloned();
            }
        }
    }
    for value in obj.values() {
        if let Some(arr) = value.as_array() {
            return arr.first().cloned();
        }
    }
    None
}

