/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.yokota.kafka.connect.transform.jsonata.utils;

import static io.yokota.kafka.connect.transform.jsonata.utils.AssertStruct.transformValue;
import static io.yokota.kafka.connect.transform.jsonata.utils.GenericAssertions.assertMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.base.Strings;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class AssertSchema {
  private AssertSchema() {
  }

  public static void assertSchema(final Schema expected, final Schema actual) {
    assertSchema(expected, actual, null);
  }

  public static void assertSchema(final Schema expected, final Schema actual, String message) {
    final String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";

    if (null == expected) {
      assertNull(actual, prefix + "actual should not be null.");
      return;
    }

    assertNotNull(expected, prefix + "expected schema should not be null.");
    assertNotNull(actual, prefix + "actual schema should not be null.");
    assertEquals(expected.name(), actual.name(), prefix + "schema.name() should match.");
    assertEquals(expected.type(), actual.type(), prefix + "schema.type() should match.");
    assertDefaultValueEquals(expected, actual, prefix + "schema.defaultValue() should match.");
    assertEquals(expected.isOptional(), actual.isOptional(), prefix + "schema.isOptional() should match.");
    assertEquals(expected.doc(), actual.doc(), prefix + "schema.doc() should match.");
    assertEquals(expected.version(), actual.version(), prefix + "schema.version() should match.");
    assertMap(expected.parameters(), actual.parameters(), prefix + "schema.parameters() should match.");

    switch (expected.type()) {
      case ARRAY:
        assertSchema(expected.valueSchema(), actual.valueSchema(), message + "valueSchema does not match.");
        break;
      case MAP:
        assertSchema(expected.keySchema(), actual.keySchema(), message + "keySchema does not match.");
        assertSchema(expected.valueSchema(), actual.valueSchema(), message + "valueSchema does not match.");
        break;
      case STRUCT:
        List<Field> expectedFields = expected.fields();
        List<Field> actualFields = actual.fields();
        assertEquals(expectedFields.size(), actualFields.size(), prefix + "Number of fields do not match.");
        for (int i = 0; i < expectedFields.size(); i++) {
          Field expectedField = expectedFields.get(i);
          Field actualField = actualFields.get(i);
          assertField(expectedField, actualField, "index " + i);
        }
        break;
    }
  }

  public static void assertDefaultValueEquals(Schema expectedSchema, Schema actualSchema, String message) {
    Object expected = transformValue(expectedSchema, expectedSchema.defaultValue());
    Object actual = transformValue(actualSchema, actualSchema.defaultValue());
    if (expected instanceof byte[] && actual instanceof byte[]) {
      assertArrayEquals((byte[]) expected, (byte[]) actual, message);
    } else {
      assertEquals(expected, actual, message);
    }
  }

  public static void assertField(final Field expected, final Field actual, String message) {
    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    if (null == expected) {
      assertNull(actual, prefix + "actual should be null.");
      return;
    }
    assertEquals(expected.name(), actual.name(), prefix + "name does not match");
    assertEquals(expected.index(), actual.index(), prefix + "name does not match");
    assertSchema(expected.schema(), actual.schema(), prefix + "schema does not match");
  }

  public static void assertField(final Field expected, final Field actual) {
    assertField(expected, actual, null);
  }

}
