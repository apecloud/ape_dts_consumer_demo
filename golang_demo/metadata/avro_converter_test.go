package metadata

import (
	"fmt"
	"testing"

	"github.com/linkedin/goavro"
	"github.com/stretchr/testify/assert"
)

func TestAvroData(t *testing.T) {
	native := mockNative()

	// valid
	error := native.SetValue("f_0", "f_0_value", Before)
	assert.Nil(t, error)

	// value type not supported
	error = native.SetValue("f_1", int(1), Before)
	assert.Equal(t, error, fmt.Errorf("value type not supported"))

	// field not exists
	error = native.SetValue("f_6", "f_6_value", Before)
	assert.Equal(t, error, fmt.Errorf("field not exists"))

	// native.After is nil
	error = native.SetValue("f_0", "f_0_value", After)
	assert.Nil(t, error)
	assert.Equal(t, len(native.After), 1)
	assert.Equal(t, native.After["f_0"], "f_0_value")
}

func TestNativeToAvro(t *testing.T) {
	avroConverter := AvroConverter{}

	nativeIn := mockNative()
	avro := avroConverter.ApeDtsToAvro(nativeIn)
	navtiveOut := avroConverter.AvroToApeDts(avro)
	assert.Equal(t, nativeIn, navtiveOut)
}

func TestNativeToBinary(t *testing.T) {
	avroConverter := AvroConverter{}

	codec, err := goavro.NewCodec(AvroSchema)
	assert.Nil(t, err)

	// mock a structed AvroData object
	nativeIn := mockNative()
	avroIn := avroConverter.ApeDtsToAvro(nativeIn)

	binary, err := codec.BinaryFromNative(nil, avroIn)
	assert.Nil(t, err)

	avroOut, _, err := codec.NativeFromBinary(binary)
	assert.Nil(t, err)
	assert.Equal(t, avroIn, avroOut)

	navtiveOut := avroConverter.AvroToApeDts(avroOut.(map[string]interface{}))
	assert.Equal(t, nativeIn, navtiveOut)
}

func mockNative() ApeDtsRecord {
	return ApeDtsRecord{
		Schema:    "schema_test",
		Tb:        "tb_test",
		Operation: "delete",
		Fields: []AvroFieldDef{
			{
				Name:       "f_0",
				AvroType:   "string",
				ColumnType: "text",
			},
			{
				Name:       "f_1",
				AvroType:   "string",
				ColumnType: "text",
			},
			{
				Name:       "f_2",
				AvroType:   "long",
				ColumnType: "int",
			},
			{
				Name:       "f_3",
				AvroType:   "double",
				ColumnType: "float",
			},
			{
				Name:       "f_4",
				AvroType:   "bytes",
				ColumnType: "binary",
			},
			{
				Name:       "f_5",
				AvroType:   "boolean",
				ColumnType: "bool",
			},
		},
		Before: map[string]interface{}{
			"f_0": nil,
			"f_1": "f_1_before",
			"f_2": int64(1),
			"f_3": 3.14,
			"f_4": []byte("bytes-value"),
			"f_5": true,
		},
		After: nil,
	}
}
