package io.yokota.kafka.connect.transform.jsonata;

import static io.yokota.kafka.connect.transform.jsonata.utils.AssertSchema.assertSchema;
import static io.yokota.kafka.connect.transform.jsonata.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class JsonataTransformationTest {
  SinkRecord record(Struct struct) {
    return record(struct, struct.schema());
  }

  SinkRecord record(Object value, Schema schema) {
    Headers headers = new ConnectHeaders();
    headers.add("key1", "value1", Schema.STRING_SCHEMA);
    headers.add("key2", "value2", Schema.STRING_SCHEMA);
    return new SinkRecord("test", 1, Schema.STRING_SCHEMA, "mykey", schema, value, 1000L,
        1234L, TimestampType.CREATE_TIME, headers);
  }

  @Test
  public void noop() {
    Schema schema = SchemaBuilder.struct()
        .field("first", Schema.STRING_SCHEMA)
        .field("last", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("first", "test")
        .put("last", "user")
        .put("email", "none@none.com");
    SinkRecord record = record(struct);

    String expr = "$";
    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, expr)
    );
    SinkRecord actual = transform.apply(record);
    assertStruct((Struct) record.value(), (Struct) actual.value());
    assertSchema(((Struct) record.value()).schema(), ((Struct) actual.value()).schema());
  }

  @Test
  public void removeEmail() {
    Schema schema = SchemaBuilder.struct()
        .field("first", Schema.STRING_SCHEMA)
        .field("last", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .optional()
        .build();
    Struct struct = new Struct(schema)
        .put("first", "test")
        .put("last", "user")
        .put("email", "none@none.com");
    SinkRecord record = record(struct);

    String expr = "(\n"
        + "    $root := $;\n"
        + "\n"
        + "    $removeEmail := function($v, $k) {$k != 'email'};\n"
        + "\n"
        + "    $newValueSchemaFields := $sift($root.valueSchema.fields, $removeEmail);\n"
        + "    $newValueSchema := $merge([$root.valueSchema, {\"fields\": $newValueSchemaFields}]);\n"
        + "\n"
        + "    $newValue := $sift($root.value, $removeEmail);\n"
        + "\n"
        + "    $newRoot := $merge([$root, {\"valueSchema\": $newValueSchema}, {\"value\": $newValue}])\n"
        + ")";
    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, expr)
    );
    SinkRecord actual = transform.apply(record);


    Schema expectedSchema = SchemaBuilder.struct()
        .field("first", Schema.STRING_SCHEMA)
        .field("last", Schema.STRING_SCHEMA)
        .optional()
        .build();
    Struct expectedStruct = new Struct(expectedSchema)
        .put("first", "test")
        .put("last", "user");
    SinkRecord expectedRecord = record(expectedStruct);

    assertStruct((Struct) expectedRecord.value(), (Struct) actual.value());
    assertSchema(((Struct) expectedRecord.value()).schema(), ((Struct) actual.value()).schema());
  }

  @Test
  public void filterTombstone() {
    Schema schema = SchemaBuilder.struct()
        .field("first", Schema.STRING_SCHEMA)
        .field("last", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .build();
    SinkRecord record = record(null, schema);

    String expr = "value = null ? null : $";
    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, expr)
    );
    SinkRecord actual = transform.apply(record);
    assertNull(actual);
  }

  @Test
  public void valueWithoutSchema() {
    SinkRecord record = record("hi", null);

    String expr = "$";
    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, expr)
    );
    SinkRecord actual = transform.apply(record);
    assertEquals(actual.value(), "hi");
  }

  @Test
  public void noopEmpty() {
    SinkRecord record = new SinkRecord(null, 1, null, null, null, null, 1000L, null, null, null);

    String expr = "$";
    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, expr)
    );
    SinkRecord actual = transform.apply(record);
    assertNull(actual.key());
    assertNull(actual.keySchema());
    assertNull(actual.value());
    assertNull(actual.valueSchema());
  }

  @Test
  public void noopComplex() {
    int dateDefVal = 100;
    int timeDefVal = 1000 * 60 * 60 * 2;
    long tsDefVal = 1000 * 60 * 60 * 24 * 365 + 100;
    java.util.Date dateDef = Date.toLogical(Date.SCHEMA, dateDefVal);
    java.util.Date timeDef = Time.toLogical(Time.SCHEMA, timeDefVal);
    java.util.Date tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal);
    BigDecimal decimalDef = new BigDecimal(BigInteger.valueOf(314159L), 5);

    Schema schema = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
        .field("int16", SchemaBuilder.int16().defaultValue((short)12).doc("int16 field").build())
        .field("int32", SchemaBuilder.int32().defaultValue(12).doc("int32 field").build())
        .field("int64", SchemaBuilder.int64().defaultValue(12L).doc("int64 field").build())
        .field("float32", SchemaBuilder.float32().defaultValue(12.2f).doc("float32 field").build())
        .field("float64", SchemaBuilder.float64().defaultValue(12.2).doc("float64 field").build())
        .field("boolean", SchemaBuilder.bool().defaultValue(true).doc("bool field").build())
        .field("string", SchemaBuilder.string().defaultValue("foo").doc("string field").build())
        .field("bytes", SchemaBuilder.bytes().defaultValue(ByteBuffer.wrap("foo".getBytes())).doc("bytes field").build())
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(Arrays.asList("a", "b", "c")).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).defaultValue(Collections.singletonMap("field", 1)).build())
        .field("date", Date.builder().defaultValue(dateDef).doc("date field").build())
        .field("time", Time.builder().defaultValue(timeDef).doc("time field").build())
        .field("ts", Timestamp.builder().defaultValue(tsDef).doc("ts field").build())
        .field("decimal", Decimal.builder(5).defaultValue(decimalDef).doc("decimal field").build())
        .build();
    // leave the struct empty so that only defaults are used
    Struct struct = new Struct(schema)
        .put("int8", (byte) 2)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", ByteBuffer.wrap("foo".getBytes()))
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1))
        .put("date", dateDef)
        .put("time", timeDef)
        .put("ts", tsDef)
        .put("decimal", decimalDef);
    SinkRecord record = record(struct);

    String expr = "$";
    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, expr)
    );
    SinkRecord actual = transform.apply(record);
    assertStruct((Struct) record.value(), (Struct) actual.value());
    assertSchema(((Struct) record.value()).schema(), ((Struct) actual.value()).schema());
  }
}
