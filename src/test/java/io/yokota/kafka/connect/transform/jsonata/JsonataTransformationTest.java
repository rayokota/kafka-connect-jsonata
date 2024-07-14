package io.yokota.kafka.connect.transform.jsonata;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class JsonataTransformationTest {
  SinkRecord record(Struct struct) {
    return new SinkRecord("test", 1, null, null, struct.schema(), struct, 1234L);
  }

  private static ObjectMapper MAPPER = new ObjectMapper();

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

    JsonataTransformation<SinkRecord> transform = new JsonataTransformation<>();
    transform.configure(
        ImmutableMap.of(JsonataTransformationConfig.EXPR_CONFIG, "")
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
        .build();
    Struct expectedStruct = new Struct(expectedSchema)
        .put("first", "test")
        .put("last", "user");
    SinkRecord expectedRecord = record(expectedStruct);

    assertStruct((Struct) expectedRecord.value(), (Struct) actual.value());
    assertSchema(((Struct) expectedRecord.value()).schema(), ((Struct) actual.value()).schema());
  }
}
