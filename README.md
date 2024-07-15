# Kafka Connect JSONata Transform

A [Kafka Connect][connect] plugin that uses a [JSONata][jsonata] expression
to transform the Kafka Connect record.

## Installation

(TBD) You can install or download the latest version of the plugin from
[Confluent Hub][confluent-hub].

### Configuration Examples

Skip tombstone records:

```
"transforms": "jsonata",
"transforms.jsonata.type": "io.yokota.kafka.connect.transform.jsonata.JsonataTransformation",
"transforms.jsonata.expr": "value = null ? null : $"
```

Drop the record key and it's schema:

```
"transforms": "jsonata",
"transforms.jsonata.type": "io.yokota.kafka.connect.transform.jsonata.JsonataTransformation",
"transforms.jsonata.expr": "$sift($, function($v, $k) {$k != 'keySchema' and $k != 'key'})"
```

For more examples, see TBD.

### Configuration Reference

#### `expr`

The JSONata expression to apply to the Kafka Connect record.

## License

This codebase is licensed under the Apache License 2.0. See the
`LICENSE` file for more details.

[confluent-hub]: https://www.confluent.io/hub/rayokota/kafka-connect-jsonata
[jsonata]: https://jsonata.org
[connect]: https://docs.confluent.io/platform/current/connect/