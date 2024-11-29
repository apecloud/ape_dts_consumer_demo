use std::collections::HashMap;

use apache_avro::Schema;
use serde::{Deserialize, Serialize};

const SCHEMA_STR: &str = r#"
{
    "type": "record",
    "name": "AvroData",
    "fields": [
        {
            "name": "schema",
            "type": "string",
            "default": ""
        },
        {
            "name": "tb",
            "type": "string",
            "default": ""
        },
        {
            "name": "operation",
            "type": "string",
            "default": ""
        },
        {
            "name": "fields",
            "default": null,
            "type": 
            [
                "null",
                {
                    "type": "array",
                    "items": {
                        "name": "AvroFieldDef",
                        "type": "record",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "column_type",
                                "type": "string",
                                "default": "string"
                            },
                            {
                                "name": "avro_type",
                                "type": "string",
                                "default": "String"
                            }
                        ]
                    }
                }
            ]
        },
        {
            "name": "before",
            "default": null,
            "type": 
            {
                "type": 
                [
                    "null",
                    {
                        "type": "map",
                        "values": 
                        [
                            "null",
                            "string",
                            "long",
                            "double",
                            "bytes",
                            "boolean"
                        ]
                    }
                ]
            }
        },
        {
            "name": "after",
            "default": null,
            "type": 
            {
                "type": 
                [
                    "null",
                    {
                        "type": "map",
                        "values": 
                        [
                            "null",
                            "string",
                            "long",
                            "double",
                            "bytes",
                            "boolean"
                        ]
                    }
                ]
            }
        },
        {
            "name": "extra",
            "default": null,
            "type": 
            {
                "type": 
                [
                    "null",
                    {
                        "type": "map",
                        "values": 
                        [
                            "null",
                            "string",
                            "long",
                            "double",
                            "bytes",
                            "boolean"
                        ]
                    }
                ]
            }
        }
    ]
}"#;

pub struct AvroConverterSchema {}

impl AvroConverterSchema {
    pub fn get_avro_schema() -> Schema {
        Schema::parse_str(SCHEMA_STR).unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ColValue {
    None,
    String(String),
    Long(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct AvroFieldDef {
    pub name: String,
    pub column_type: String,
    pub avro_type: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApeDtsRecord {
    pub schema: String,
    pub tb: String,
    pub operation: String,
    pub fields: Vec<AvroFieldDef>,
    pub before: HashMap<String, ColValue>,
    pub after: HashMap<String, ColValue>,
    pub extra: HashMap<String, ColValue>,
}
