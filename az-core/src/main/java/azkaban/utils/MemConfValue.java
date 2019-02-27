package azkaban.utils;

import azkaban.Constants;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public class MemConfValue {

  private final String string;
  private final long size;

  public static MemConfValue parseMaxXms(final Props props) {
    return parse(props,
        Constants.JobProperties.JOB_MAX_XMS, Constants.JobProperties.MAX_XMS_DEFAULT);
  }

  public static MemConfValue parseMaxXmx(final Props props) {
    return parse(props,
        Constants.JobProperties.JOB_MAX_XMX, Constants.JobProperties.MAX_XMX_DEFAULT);
  }

  private static MemConfValue parse(final Props props, final String key,
      final String defaultValue) {
    final String stringValue = props.getString(key, defaultValue);
    Preconditions.checkArgument(!StringUtils.isBlank(stringValue),
        String.format("%s must not have an empty value. "
            + "Remove the property to use default or specify a valid value.", key));
    final long size = Utils.parseMemString(stringValue);
    return new MemConfValue(stringValue, size);
  }

  private MemConfValue(final String string, final long size) {
    this.string = string;
    this.size = size;
  }

  public String getString() {
    return this.string;
  }

  public long getSize() {
    return this.size;
  }

}
