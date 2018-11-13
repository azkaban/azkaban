package azkaban.jobtype;

import org.junit.Test;

import azkaban.jobtype.javautils.ValidationUtils;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;

public class TestValidationUtils {

  private static final Props PROPS = new Props();
  static {
    PROPS.put("a", "a");
    PROPS.put("b", "b");
    PROPS.put("c", "c");
    PROPS.put("d", "d");
  }

  @Test
  public void testAllExistSucess() {
    String[] keys = {"a", "b", "c", "d"};
    ValidationUtils.validateAllOrNone(PROPS, keys);
    ValidationUtils.validateAllNotEmpty(PROPS, keys);
  }

  @Test(expected=UndefinedPropertyException.class)
  public void testAllExistFail() {
    ValidationUtils.validateAllNotEmpty(PROPS, "x", "y");
  }

  @Test(expected=UndefinedPropertyException.class)
  public void testAllExistFail2() {
    ValidationUtils.validateAllNotEmpty(PROPS, "a", "y");
  }

  @Test
  public void testNoneExistSuccess() {
    ValidationUtils.validateAllOrNone(PROPS, "z");
    ValidationUtils.validateAllOrNone(PROPS, "e", "f", "g");
  }
}
