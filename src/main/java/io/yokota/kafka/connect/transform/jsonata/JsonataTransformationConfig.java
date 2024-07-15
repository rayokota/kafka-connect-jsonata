package io.yokota.kafka.connect.transform.jsonata;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class JsonataTransformationConfig extends AbstractConfig {

  public final String expr;
  public final long timeoutMs;
  public final int maxDepth;

  public JsonataTransformationConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.expr = getString(EXPR_CONFIG);
    this.timeoutMs = getLong(TIMEOUT_MS_CONFIG);
    this.maxDepth = getInt(MAX_DEPTH_CONFIG);
  }

  public static final String EXPR_CONFIG = "expr";
  static final String EXPR_DOC = "JSONata transformation expression.";

  public static final String TIMEOUT_MS_CONFIG = "timeout.ms";
  static final String TIMEOUT_MS_DOC = "The maximum amount of time to wait (in ms) "
      + "for the JSONata transformation to complete.";

  public static final String MAX_DEPTH_CONFIG = "max.depth";
  static final String MAX_DEPTH_DOC = "The maximum number of recursive calls allowed "
      + "for the JSONata transformation.";

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            EXPR_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            EXPR_DOC
        )
        .define(
            TIMEOUT_MS_CONFIG,
            ConfigDef.Type.LONG,
            5000,
            ConfigDef.Importance.MEDIUM,
            TIMEOUT_MS_DOC
        )
        .define(
            MAX_DEPTH_CONFIG,
            ConfigDef.Type.INT,
            1000,
            ConfigDef.Importance.MEDIUM,
            MAX_DEPTH_DOC
        );
  }
}
