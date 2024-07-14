package io.yokota.kafka.connect.transform.jsonata;

import static org.apache.kafka.connect.data.Values.convertToInteger;
import static org.apache.kafka.connect.data.Values.convertToLong;
import static org.apache.kafka.connect.data.Values.parseString;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonataTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(JsonataTransformation.class);

  private static final int DEFAULT_CACHE_SIZE = 100;

  private final LoadingCache<String, Expressions> cache;

  public JsonataTransformation() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<String, Expressions>() {
          @Override
          public Expressions load(String expr) throws Exception {
            try {
              return Expressions.parse(expr);
            } catch (ParseException e) {
              throw new DataException("Could not parse expression", e);
            } catch (EvaluateRuntimeException ere) {
              throw new DataException("Could not evaluate expression", ere);
            } catch (JsonProcessingException e) {
              throw new DataException("Could not parse message", e);
            } catch (IOException e) {
              throw new DataException(e);
            }
          }
        });
  }

  @Override
  public ConfigDef config() {
    return JsonataTransformationConfig.config();
  }

  @Override
  public R apply(R record) {
    JsonNode jsonObj = recordToJsonNode(record);
    JsonNode result = jsonObj;
    String exprStr = config.expr;
    if (exprStr != null && !exprStr.trim().isEmpty()) {
      Expressions expr;
      try {
        expr = cache.get(exprStr);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof DataException) {
          throw (DataException) e.getCause();
        } else {
          throw new DataException("Could not get expression", e.getCause());
        }
      }
      try {
        result = expr.evaluate(jsonObj, config.timeoutMs, config.maxDepth);
      } catch (EvaluateException e) {
        throw new DataException("Could not evaluate expression", e);
      }
    }

    try {
      ObjectMapper mapper = new ObjectMapper();
      String pretty = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObj);
      System.out.println(pretty);
    } catch (Exception e) {
    }

    return jsonNodeToRecord(record, result);
  }

  @Override
  public void close() {
  }

  JsonataTransformationConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new JsonataTransformationConfig(settings);
  }

  private JsonNode recordToJsonNode(R record) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("topic", record.topic());
    node.put("kafkaPartition", record.kafkaPartition());
    if (record.keySchema() != null) {
      node.set("keySchema", schemaToJsonNode(record.keySchema()));
    }
    if (record.key() != null) {
      node.set("key", objectToJsonNode(record.key()));
    }
    if (record.valueSchema() != null) {
      node.set("valueSchema", schemaToJsonNode(record.valueSchema()));
    }
    if (record.value() != null) {
      node.set("value", objectToJsonNode(record.value()));
    }
    node.put("timestamp", record.timestamp());
    if (record.headers() != null) {
      node.set("headers", headersToJsonNode(record.headers()));
    }
    return node;
  }

  private JsonNode schemaToJsonNode(Schema schema) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    Schema.Type type = schema.type();
    node.put("type", type.name());
    node.put("optional", schema.isOptional());
    if (schema.defaultValue() != null) {
      node.set("defaultValue", objectToJsonNode(schema.defaultValue()));
    }
    node.put("name", schema.name());
    node.put("version", schema.version());
    node.put("doc", schema.doc());
    if (schema.parameters() != null) {
      node.set("parameters", mapToJsonNode(schema.parameters()));
    }
    if (type == Schema.Type.MAP && schema.keySchema() != null) {
      node.set("keySchema", schemaToJsonNode(schema.keySchema()));
    }
    if ((type == Schema.Type.MAP || type == Schema.Type.ARRAY)
        && schema.valueSchema() != null) {
      node.set("valueSchema", schemaToJsonNode(schema.valueSchema()));
    }
    if (type == Schema.Type.STRUCT && schema.fields() != null) {
      node.set("fields", fieldsToJsonNode(schema.fields()));
    }
    return node;
  }

  private JsonNode mapToJsonNode(Map<String, String> map) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      node.put(entry.getKey(), entry.getValue());
    }
    return node;
  }

  private JsonNode objectToJsonNode(Object value) {
    if (value == null) {
      return NullNode.getInstance();
    } else if (value instanceof List) {
      List<?> list = (List<?>) value;
      ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode(list.size());
      list.forEach(v -> arrayNode.add(objectToJsonNode(v)));
    } else if (value instanceof Boolean) {
      return JsonNodeFactory.instance.booleanNode((Boolean) value);
    } else if (value instanceof BigDecimal) {
      return JsonNodeFactory.instance.numberNode((BigDecimal) value);
    } else if (value instanceof BigInteger) {
      return JsonNodeFactory.instance.numberNode((BigInteger) value);
    } else if (value instanceof Long) {
      return JsonNodeFactory.instance.numberNode((Long) value);
    } else if (value instanceof Double) {
      return JsonNodeFactory.instance.numberNode((Double) value);
    } else if (value instanceof Float) {
      return JsonNodeFactory.instance.numberNode((Float) value);
    } else if (value instanceof Integer) {
      return JsonNodeFactory.instance.numberNode((Integer) value);
    } else if (value instanceof Short) {
      return JsonNodeFactory.instance.numberNode((Short) value);
    } else if (value instanceof Byte) {
      return JsonNodeFactory.instance.numberNode((Byte) value);
    } else if (value instanceof byte[]) {
      return JsonNodeFactory.instance.binaryNode((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return JsonNodeFactory.instance.binaryNode(getBytesFromByteBuffer((ByteBuffer) value));
    } else if (value instanceof java.util.Date) {
      java.util.Date date = (java.util.Date) value;
      String formatted = Values.dateFormatFor(date).format(date);
      return JsonNodeFactory.instance.textNode(formatted);
    } else if (value instanceof Struct) {
      Struct struct = (Struct) value;
      ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
      for (Field field : struct.schema().fields()) {
        objectNode.set(field.name(), objectToJsonNode(struct.get(field)));
      }
      return objectNode;
    } else if (value instanceof String) {
      return JsonNodeFactory.instance.textNode((String) value);
    }
    throw new DataException("Unsupported type " + value.getClass().getName());
  }

  private byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }

    byteBuffer.rewind();
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes;
  }

  private JsonNode headersToJsonNode(Headers headers) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    for (Header header : headers) {
      node.set(header.key(), headerToJsonNode(header));
    }
    return node;
  }

  private JsonNode headerToJsonNode(Header header) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("key", header.key());
    if (header.value() != null) {
      node.set("value", objectToJsonNode(header.value()));
    }
    if (header.schema() != null) {
      node.set("schema", schemaToJsonNode(header.schema()));
    }
    return node;
  }

  private JsonNode fieldsToJsonNode(Iterable<Field> fields) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    for (Field field : fields) {
      node.set(field.name(), fieldToJsonNode(field));
    }
    return node;
  }

  private JsonNode fieldToJsonNode(Field field) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("name", field.name());
    node.put("index", field.index());
    if (field.schema() != null) {
      node.set("schema", schemaToJsonNode(field.schema()));
    }
    return node;
  }

  @SuppressWarnings("unchecked")
  private R jsonNodeToRecord(R originalRecord, JsonNode node) {
    String topic = node.get("topic").asText();
    int kafkaPartition = node.get("kafkaPartition").asInt();
    Schema keySchema = null;
    if (node.hasNonNull("keySchema")) {
      keySchema = jsonNodeToSchema(node.get("keySchema"));
    }
    Object key = null;
    if (node.hasNonNull("key")) {
      key = jsonNodeToObject(keySchema, node.get("key"));
    }
    Schema valueSchema = null;
    if (node.hasNonNull("valueSchema")) {
      valueSchema = jsonNodeToSchema(node.get("valueSchema"));
    }
    Object value = null;
    if (node.hasNonNull("value")) {
      value = jsonNodeToObject(valueSchema, node.get("value"));
    }
    long timestamp = node.get("timestamp").asLong();
    Headers headers = null;
    if (node.hasNonNull("headers")) {
      headers = jsonNodeToHeaders(node.get("headers"));
    }
    if (originalRecord instanceof SourceRecord) {
      SourceRecord source = (SourceRecord) originalRecord;
      return (R) new SourceRecord(
          source.sourcePartition(),
          source.sourceOffset(),
          topic,
          kafkaPartition,
          keySchema,
          key,
          valueSchema,
          value,
          timestamp,
          headers
      );
    } else {
      SinkRecord sink = (SinkRecord) originalRecord;
      return (R) new SinkRecord(
          topic,
          kafkaPartition,
          keySchema,
          key,
          valueSchema,
          value,
          sink.kafkaOffset(),
          timestamp,
          sink.timestampType(),
          headers
      );
    }
  }

  private Schema jsonNodeToSchema(JsonNode node) {
    SchemaBuilder builder;
    if (node.hasNonNull("valueSchema")) {
      Schema valueSchema = jsonNodeToSchema(node.get("valueSchema"));
      if (node.hasNonNull("keySchema")) {
        Schema keySchema = jsonNodeToSchema(node.get("keySchema"));
        builder = SchemaBuilder.map(keySchema, valueSchema);
      } else {
        builder = SchemaBuilder.array(valueSchema);
      }
    } else {
      Schema.Type type = Schema.Type.valueOf(node.get("type").asText());
      builder = SchemaBuilder.type(type);
    }
    if (node.hasNonNull("optional")) {
      if (node.get("optional").asBoolean()) {
        builder.optional();
      }
    }
    if (node.hasNonNull("name")) {
      builder.name(node.get("name").asText());
    }
    if (node.hasNonNull("version")) {
      builder.version(node.get("version").asInt());
    }
    if (node.hasNonNull("doc")) {
      builder.doc(node.get("doc").asText());
    }
    if (node.hasNonNull("parameters")) {
      Map<String, String> parameters = new LinkedHashMap<>();
      node.get("parameters").fields().forEachRemaining(entry ->
          parameters.put(entry.getKey(), entry.getValue().asText()));
      builder.parameters(parameters);
    }
    if (node.hasNonNull("fields")) {
      node.get("fields").fields().forEachRemaining(entry ->
          builder.field(entry.getKey(), jsonNodeToField(entry.getValue()).schema()));
    }
    if (node.hasNonNull("defaultValue")) {
      builder.defaultValue(jsonNodeToObject(builder, node.get("defaultValue")));
    }
    return builder.build();
  }

  private Field jsonNodeToField(JsonNode node) {
    return new Field(
        node.get("name").asText(),
        node.get("index").asInt(),
        node.hasNonNull("schema") ? jsonNodeToSchema(node.get("schema")) : null
    );
  }

  private Headers jsonNodeToHeaders(JsonNode node) {
    Headers headers = new ConnectHeaders();
    node.fields().forEachRemaining(entry -> {
      JsonNode header = entry.getValue();
      Schema schema = null;
      if (header.hasNonNull("schema")) {
        schema = jsonNodeToSchema(header.get("schema"));
      }
      Object value = jsonNodeToObject(schema, header.get("value"));
      headers.add(entry.getKey(), value, schema);
    });
    return headers;
  }

  private Object jsonNodeToObject(Schema schema, JsonNode node) {
    Object object = getObject(schema, node);
    switch (schema.type()) {
      case INT8:
        return Values.convertToByte(schema, object);
      case INT16:
        return Values.convertToShort(schema, object);
      case INT32:
        return convertMaybeLogicalInteger(schema, object);
      case INT64:
        return convertMaybeLogicalLong(schema, object);
      case FLOAT32:
        return Values.convertToFloat(schema, object);
      case FLOAT64:
        return Values.convertToDouble(schema, object);
      case BOOLEAN:
        return Values.convertToBoolean(schema, object);
      case STRING:
        return Values.convertToString(schema, object);
      case BYTES:
        return convertMaybeLogicalBytes(schema, object);
      case ARRAY:
        return Values.convertToList(schema, object);
      case MAP:
        return Values.convertToMap(schema, object);
      case STRUCT:
        return Values.convertToStruct(schema, object);
      default:
        throw new DataException("Unsupported type " + schema.type());
    }
  }

  private static Serializable convertMaybeLogicalBytes(Schema toSchema, Object value) {
    if (toSchema != null) {
      if (Decimal.LOGICAL_NAME.equals(toSchema.name())) {
        return convertToDecimal(toSchema, value);
      }
    }
    return convertToBytes(toSchema, value);
  }

  private static BigDecimal convertToDecimal(Schema toSchema, Object value) {
    if (value instanceof ByteBuffer) {
      value = Utils.toArray((ByteBuffer) value);
    }
    if (value instanceof byte[]) {
      return Decimal.toLogical(toSchema, (byte[]) value);
    }
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof Number) {
      // Not already a decimal, so treat it as a double ...
      double converted = ((Number) value).doubleValue();
      return BigDecimal.valueOf(converted);
    }
    if (value instanceof String) {
      return new BigDecimal(value.toString());
    }
    throw new DataException(
        "Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
  }

  private static byte[] convertToBytes(Schema toSchema, Object value) {
    if (value instanceof ByteBuffer) {
      return Utils.toArray((ByteBuffer) value);
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    throw new DataException(
        "Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
  }

  private static Serializable convertMaybeLogicalInteger(Schema toSchema, Object value) {
    if (toSchema != null) {
      if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(toSchema.name())) {
        return convertToDate(toSchema, value);
      }
      if (Time.LOGICAL_NAME.equals(toSchema.name())) {
        return convertToTime(toSchema, value);
      }
    }
    return convertToInteger(toSchema, value);
  }

  private static java.util.Date convertToDate(Schema toSchema, Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      SchemaAndValue parsed = parseString(value.toString());
      value = parsed.value();
    }
    if (value instanceof java.util.Date) {
      return (java.util.Date) value;
    }
    long numeric = asLong(value);
    return Date.toLogical(toSchema, (int) numeric);
  }

  private static java.util.Date convertToTime(Schema toSchema, Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      SchemaAndValue parsed = parseString(value.toString());
      value = parsed.value();
    }
    if (value instanceof java.util.Date) {
      return (java.util.Date) value;
    }
    long numeric = asLong(value);
    return Time.toLogical(toSchema, (int) numeric);
  }

  private static Serializable convertMaybeLogicalLong(Schema toSchema, Object value) {
    if (toSchema != null) {
      if (Timestamp.LOGICAL_NAME.equals(toSchema.name())) {
        return convertToTimestamp(toSchema, value);
      }
    }
    return convertToLong(toSchema, value);
  }

  private static java.util.Date convertToTimestamp(Schema toSchema, Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      SchemaAndValue parsed = parseString(value.toString());
      value = parsed.value();
    }
    if (value instanceof java.util.Date) {
      return (java.util.Date) value;
    }
    long numeric = asLong(value);
    return Timestamp.toLogical(toSchema, numeric);
  }

  protected static long asLong(Object value) {
    try {
      if (value instanceof Number) {
        Number number = (Number) value;
        return number.longValue();
      }
      if (value instanceof String) {
        return new BigDecimal(value.toString()).longValue();
      }
    } catch (NumberFormatException e) {
      // fall through
    }
    throw new DataException(
        "Unable to convert " + value + " (" + value.getClass() + ") to a number");
  }

  private Object getObject(Schema schema, JsonNode node) {
    if (node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      return node.numberValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isBinary()) {
      try {
        return node.binaryValue();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    if (node.isArray()) {
      ArrayNode arrayNode = (ArrayNode) node;
      List<Object> list = new ArrayList<>(arrayNode.size());
      arrayNode.fields().forEachRemaining(entry ->
          list.add(getObject(schema.valueSchema(), entry.getValue())));
      return list;
    }
    if (node.isObject()) {
      switch (schema.type()) {
        case MAP:
          Map<String, Object> map = new LinkedHashMap<>();
          node.fields().forEachRemaining(entry ->
              map.put(entry.getKey(), getObject(schema.valueSchema(), entry.getValue())));
          return map;
        case STRUCT:
          ObjectNode objectNode = (ObjectNode) node;
          Struct struct = new Struct(schema);
          schema.fields().forEach(field ->
              struct.put(field, getObject(field.schema(), objectNode.get(field.name()))));
          return struct;
      }
    }
    return node.textValue();
  }
}
