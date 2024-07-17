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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DateFormat;
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

    /*
    try {
      ObjectMapper mapper = new ObjectMapper();
      String pretty = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObj);
      System.out.println(pretty);
    } catch (Exception e) {
    }
    */

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
    if (record.timestamp() != null) {
      node.put("timestamp", record.timestamp());
    }
    if (record.headers() != null) {
      node.set("headers", headersToJsonNode(record.headers()));
    }
    return node;
  }

  private JsonNode schemaToJsonNode(Schema schema) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    Schema.Type type = schema.type();
    node.put("type", type.name());
    if (schema.isOptional()) {
      node.put("optional", schema.isOptional());
    }
    if (schema.defaultValue() != null) {
      node.set("defaultValue", objectToJsonNode(schema.defaultValue()));
    }
    if (schema.name() != null) {
      node.put("name", schema.name());
    }
    if (schema.version() != null) {
      node.put("version", schema.version());
    }
    if (schema.doc() != null) {
      node.put("doc", schema.doc());
    }
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
      return arrayNode;
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
      map.entrySet().forEach(entry ->
          objectNode.put(entry.getKey().toString(), objectToJsonNode(entry.getValue())));
      return objectNode;
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
      return JsonNodeFactory.instance.binaryNode(Utils.toArray((ByteBuffer) value));
    } else if (value instanceof java.util.Date) {
      java.util.Date date = (java.util.Date) value;
      DateFormat dateFormat = Values.dateFormatFor(date);
      String formatted = dateFormat.format(date);
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

  private JsonNode headersToJsonNode(Headers headers) {
    ArrayNode node = JsonNodeFactory.instance.arrayNode();
    for (Header header : headers) {
      node.add(headerToJsonNode(header));
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
    if (node.isNull()) {
      return null;
    }
    String topic = null;
    if (node.hasNonNull("topic")) {
      topic = node.get("topic").asText();
    }
    int kafkaPartition = -1;
    if (node.hasNonNull("kafkaPartition")) {
      kafkaPartition = node.get("kafkaPartition").asInt();
    }
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
    Long timestamp = null;
    if (node.hasNonNull("timestamp")) {
      timestamp = node.get("timestamp").asLong();
    }
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
    if (node.isNull()) {
      return null;
    }
    SchemaBuilder builder;
    if (node.hasNonNull("valueSchema")) {
      Schema valueSchema = jsonNodeToSchema(node.get("valueSchema"));
      if (node.hasNonNull("keySchema")) {
        Schema keySchema = jsonNodeToSchema(node.get("keySchema"));
        builder = SchemaBuilder.map(keySchema, valueSchema);
      } else {
        builder = SchemaBuilder.array(valueSchema);
      }
    } else if (node.hasNonNull("type")) {
      Schema.Type type = Schema.Type.valueOf(node.get("type").asText());
      builder = SchemaBuilder.type(type);
    } else {
      throw new DataException("Schema node must have 'type' or 'valueSchema'");
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
    if (node.isNull()) {
      return null;
    }
    if (node.hasNonNull("name") && node.hasNonNull("index")) {
      return new Field(
          node.get("name").asText(),
          node.get("index").asInt(),
          node.hasNonNull("schema") ? jsonNodeToSchema(node.get("schema")) : null
      );
    } else {
      throw new DataException("Field node must have 'name' and 'index'");
    }
  }

  private Headers jsonNodeToHeaders(JsonNode node) {
    if (node.isNull()) {
      return null;
    }
    Headers headers = new ConnectHeaders();
    node.elements().forEachRemaining(item -> {
      Schema schema = null;
      if (item.hasNonNull("schema")) {
        schema = jsonNodeToSchema(item.get("schema"));
      }
      Object value = jsonNodeToObject(schema, item.get("value"));
      headers.add(item.get("key").asText(), value, schema);
    });
    return headers;
  }

  private Object jsonNodeToObject(Schema schema, JsonNode node) {
    if (node.isNull()) {
      return null;
    }
    try {
      switch (schema.type()) {
        case INT8:
          return node.numberValue().byteValue();
        case INT16:
          return node.shortValue();
        case INT32:
          return convertMaybeLogicalInteger(schema,
              node.isNumber() ? node.intValue() : node.textValue());
        case INT64:
          return convertMaybeLogicalLong(schema,
              node.isNumber() ? node.longValue() : node.textValue());
        case FLOAT32:
          return node.floatValue();
        case FLOAT64:
          return node.doubleValue();
        case BOOLEAN:
          return node.booleanValue();
        case STRING:
          return node.textValue();
        case BYTES:
          return convertMaybeLogicalBytes(schema,
              node.isBinary() ? node.binaryValue() :
                  (node.isNumber() ? node.numberValue() : node.textValue()));
        case ARRAY:
          ArrayNode arrayNode = (ArrayNode) node;
          List<Object> list = new ArrayList<>(arrayNode.size());
          arrayNode.elements().forEachRemaining(item ->
              list.add(jsonNodeToObject(schema.valueSchema(), item)));
          return list;
        case MAP:
          Map<String, Object> map = new LinkedHashMap<>();
          node.fields().forEachRemaining(entry ->
              map.put(entry.getKey(), jsonNodeToObject(schema.valueSchema(), entry.getValue())));
          return map;
        case STRUCT:
          ObjectNode objectNode = (ObjectNode) node;
          Struct struct = new Struct(schema);
          schema.fields().forEach(field ->
              struct.put(field, jsonNodeToObject(field.schema(), objectNode.get(field.name()))));
          return struct;
        default:
          throw new DataException("Unsupported type " + schema.type());
      }
    } catch (IOException e) {
      throw new DataException("Could not convert node to object", e);
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
}
