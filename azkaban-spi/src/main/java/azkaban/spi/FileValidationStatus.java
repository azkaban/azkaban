package azkaban.spi;

import java.util.HashMap;
import java.util.Map;


// REMOVED = validator removed this file, it is blacklisted
// VALID = validator gave this file the green light - no modifications made, it's fine as is.
// NEW = not yet processed by the validator
public enum FileValidationStatus {
  REMOVED(0), VALID(1), NEW(2);

  private final int value;
  private static Map map = new HashMap<>();

  FileValidationStatus(final int newValue) {
    value = newValue;
  }

  static {
    for (FileValidationStatus v : FileValidationStatus.values()) {
      map.put(v.value, v);
    }
  }

  public static FileValidationStatus valueOf(int v) {
    return (FileValidationStatus) map.get(v);
  }

  public int getValue() { return value; }
}
