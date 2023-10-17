import fastavro

field_def_schema_json = {
    "type": "record",
    "name": "AvroFieldDef",
    "fields": [
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "type_name",
            "type": "string",
            "default": "string"
        }
    ]
}

schema_json = {
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
            "default": "insert"
        },
        {
            "name": "fields",
            "default": None,
            "type": 
            [
                "null",
                {
                    "type": "array",
                    "items": field_def_schema_json
                }
            ]
        },
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": ["null", "string", "long", "double", "bytes", "boolean"]
                }
            ],
            "default": None
        },
        {
            "name": "after",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": ["null", "string", "long", "double", "bytes", "boolean"]
                }
            ],
            "default": None
        }
    ],
}

def get_avro_schema():
    return fastavro.parse_schema(schema_json)