package metadata

const Schema string = "schema"
const Tb string = "tb"
const Operation string = "operation"
const Before string = "before"
const After string = "after"
const Extra string = "extra"
const Fields string = "fields"

const Name string = "name"
const AvroType string = "avro_type"
const ColumnType string = "column_type"

const TypeArray string = "array"
const TypeMap string = "map"
const TypeString string = "string"
const TypeLong string = "long"
const TypeDouble string = "double"
const TypeBytes string = "bytes"
const TypeBoolean string = "boolean"

// AvroConverter ...
type AvroConverter struct {
}

// AvroToApeDts ...
func (o *AvroConverter) AvroToApeDts(avro map[string]interface{}) ApeDtsRecord {
	return ApeDtsRecord{
		Schema:    avro[Schema].(string),
		Tb:        avro[Tb].(string),
		Operation: avro[Operation].(string),
		Before:    o.avroFieldValuesToNative(avro, Before),
		After:     o.avroFieldValuesToNative(avro, After),
		Extra:     o.avroFieldValuesToNative(avro, Extra),
		Fields:    o.avroFieldsToNative(avro),
	}
}

// ApeDtsToAvro ...
func (o *AvroConverter) ApeDtsToAvro(native ApeDtsRecord) map[string]interface{} {
	return map[string]interface{}{
		Schema:    native.Schema,
		Tb:        native.Tb,
		Operation: native.Operation,
		Before:    o.nativeFieldValuesToAvro(native.Before),
		After:     o.nativeFieldValuesToAvro(native.After),
		Extra:     o.nativeFieldValuesToAvro(native.Extra),
		Fields:    o.nativeFieldToAvro(native.Fields),
	}
}

func (o *AvroConverter) avroFieldValuesToNative(avro map[string]interface{}, field string) map[string]interface{} {
	if avro == nil || avro[field] == nil {
		return nil
	}

	data := avro[field].(map[string]interface{})
	if data[TypeMap] == nil {
		return nil
	}

	avroFieldValues := data[TypeMap].(map[string]interface{})
	fieldValues := map[string]interface{}{}

	for field, avroValue := range avroFieldValues {
		if avroValue == nil {
			fieldValues[field] = nil
			continue
		}

		item := avroValue.(map[string]interface{})
		value := (interface{})(nil)
		if item[TypeString] != nil {
			value = item[TypeString]
		} else if item[TypeLong] != nil {
			value = item[TypeLong]
		} else if item[TypeDouble] != nil {
			value = item[TypeDouble]
		} else if item[TypeBytes] != nil {
			value = item[TypeBytes]
		} else if item[TypeBoolean] != nil {
			value = item[TypeBoolean]
		}
		fieldValues[field] = value
	}

	return fieldValues
}

func (o *AvroConverter) nativeFieldValuesToAvro(nativeFieldValues map[string]interface{}) interface{} {
	if nativeFieldValues == nil {
		return nil
	}

	array := map[string]interface{}{}

	for field, nativeValue := range nativeFieldValues {
		if nativeValue == nil {
			array[field] = nativeValue
			continue
		}

		switch nativeValue.(type) {
		case string:
			array[field] = map[string]interface{}{TypeString: nativeValue}
		case int64:
			array[field] = map[string]interface{}{TypeLong: nativeValue}
		case float64:
			array[field] = map[string]interface{}{TypeDouble: nativeValue}
		case []byte:
			array[field] = map[string]interface{}{TypeBytes: nativeValue}
		case bool:
			array[field] = map[string]interface{}{TypeBoolean: nativeValue}
		}
	}
	return map[string]interface{}{TypeMap: array}
}

func (o *AvroConverter) avroFieldsToNative(avro map[string]interface{}) []AvroFieldDef {
	if avro == nil || avro[Fields] == nil {
		return nil
	}

	data := avro[Fields].(map[string]interface{})
	if data[TypeArray] == nil {
		return nil
	}

	avroFields := data[TypeArray].([]interface{})
	fields := []AvroFieldDef{}
	for i := 0; i < len(avroFields); i++ {
		item := avroFields[i].(map[string]interface{})
		field := AvroFieldDef{
			Name:       item[Name].(string),
			AvroType:   item[AvroType].(string),
			ColumnType: item[ColumnType].(string),
		}
		fields = append(fields, field)
	}
	return fields
}

func (o *AvroConverter) nativeFieldToAvro(nativeFields []AvroFieldDef) interface{} {
	if nativeFields == nil {
		return nil
	}

	array := []interface{}{}
	for i := 0; i < len(nativeFields); i++ {
		avroField := map[string]interface{}{
			Name:       nativeFields[i].Name,
			AvroType:   nativeFields[i].AvroType,
			ColumnType: nativeFields[i].ColumnType}
		array = append(array, avroField)
	}
	return map[string]interface{}{TypeArray: array}
}
