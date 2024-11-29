use std::{collections::HashMap, io::Cursor};

use apache_avro::{from_avro_datum, types::Value, Schema};

use super::avro_schema::{ApeDtsRecord, AvroConverterSchema, AvroFieldDef, ColValue};

const SCHEMA: &str = "schema";
const TB: &str = "tb";
const OPERATION: &str = "operation";
const BEFORE: &str = "before";
const AFTER: &str = "after";
const EXTRA: &str = "extra";
const FIELDS: &str = "fields";

pub struct AvroConverter {
    avro_schema: Schema,
}

impl AvroConverter {
    pub fn new() -> Self {
        Self {
            avro_schema: AvroConverterSchema::get_avro_schema(),
        }
    }

    pub fn avro_to_ape_dts(&self, data: &[u8]) -> anyhow::Result<ApeDtsRecord> {
        // Deserialize the binary data using the Avro schema
        let mut cursor = Cursor::new(data);
        let value = from_avro_datum(&self.avro_schema, &mut cursor, None)?;

        let mut avro_map = Self::avro_to_map(value);
        let avro_to_string = |value: Option<Value>| {
            if let Some(Value::String(v)) = value {
                return v;
            }
            String::new()
        };

        let schema = avro_to_string(avro_map.remove(SCHEMA));
        let tb = avro_to_string(avro_map.remove(TB));
        let operation = avro_to_string(avro_map.remove(OPERATION));

        let fields = self.avro_to_fields(avro_map.remove(FIELDS));
        let extra = self.avro_to_col_values(avro_map.remove(EXTRA));
        let before = self.avro_to_col_values(avro_map.remove(BEFORE));
        let after = self.avro_to_col_values(avro_map.remove(AFTER));
        Ok(ApeDtsRecord {
            schema,
            tb,
            operation,
            fields,
            before,
            after,
            extra,
        })
    }

    fn avro_to_map(value: Value) -> HashMap<String, Value> {
        let mut avro_map = HashMap::new();
        if let Value::Record(record) = value {
            for (field, value) in record {
                avro_map.insert(field, value);
            }
        }
        avro_map
    }

    fn avro_to_col_values(&self, value: Option<Value>) -> HashMap<String, ColValue> {
        // Some(Union(1, Map({
        //     "bytes_col": Union(4, Bytes([5, 6, 7, 8])),
        //     "string_col": Union(1, String("string_after")),
        //     "boolean_col": Union(5, Boolean(true)),
        //     "long_col": Union(2, Long(2)),
        //     "null_col": Union(0, Null),
        //     "double_col": Union(3, Double(2.2))
        //   })))
        if let Some(Value::Union(1, v)) = value {
            if let Value::Map(map_v) = *v {
                let mut col_values = HashMap::new();
                for (col, value) in map_v {
                    col_values.insert(col, Self::avro_to_col_value(value));
                }
                return col_values;
            }
        }
        HashMap::new()
    }

    fn avro_to_fields(&self, value: Option<Value>) -> Vec<AvroFieldDef> {
        if let Some(v) = value {
            return apache_avro::from_value(&v).unwrap();
        }
        vec![]
    }

    fn avro_to_col_value(value: Value) -> ColValue {
        match value {
            Value::Long(v) => ColValue::Long(v),
            Value::Double(v) => ColValue::Double(v),
            Value::Bytes(v) => ColValue::Bytes(v),
            Value::String(v) => ColValue::String(v),
            Value::Boolean(v) => ColValue::Boolean(v),
            Value::Null => ColValue::None,
            Value::Union(_, v) => Self::avro_to_col_value(*v),
            // NOT supported
            _ => ColValue::None,
        }
    }
}
