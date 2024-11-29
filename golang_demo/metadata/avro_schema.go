package metadata

import "fmt"

// AvroSchema ... the avro schema json for AvroData
const AvroSchema string = `
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
}
`

// AvroFieldDef ...
type AvroFieldDef struct {
	Name       string `avro:"name"`
	ColumnType string `avro:"column_type,default=string"`
	AvroType   string `avro:"avro_type,default=string"`
}

// ApeDtsRecord ...
type ApeDtsRecord struct {
	Schema    string                 `avro:"schema"`
	Tb        string                 `avro:"tb"`
	Operation string                 `avro:"operation"`
	Fields    []AvroFieldDef         `avro:"fields,omitempty"`
	Before    map[string]interface{} `avro:"before,omitempty"`
	After     map[string]interface{} `avro:"after,omitempty"`
	Extra     map[string]interface{} `avro:"extra,omitempty"`
}

// AddField ...
func (o *ApeDtsRecord) AddField(name string, columnType string, avroType string) {
	o.Fields = append(o.Fields, AvroFieldDef{Name: name, ColumnType: columnType, AvroType: avroType})
}

// SetValue ...
func (o *ApeDtsRecord) SetValue(field string, value interface{}, image string) error {
	var values map[string]interface{}
	if image == Before {
		if o.Before == nil {
			o.Before = map[string]interface{}{}
		}
		values = o.Before
	} else {
		if o.After == nil {
			o.After = map[string]interface{}{}
		}
		values = o.After
	}

	fieldExists := false
	for i := range o.Fields {
		if o.Fields[i].Name == field {
			fieldExists = true
			break
		}
	}
	if !fieldExists {
		return fmt.Errorf("field not exists")
	}

	if value == nil {
		values[field] = nil
		return nil
	}

	switch value.(type) {
	case string, int64, float64, []byte, bool:
		values[field] = value
		return nil
	default:
		return fmt.Errorf("value type not supported")
	}
}
