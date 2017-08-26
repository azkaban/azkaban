package azkaban.project.validator;

import static org.junit.Assert.assertEquals;

import azkaban.utils.Props;
import com.google.common.io.Resources;
import java.net.URL;
import org.junit.Test;

public class XmlValidatorManagerTest {

  private final Props baseProps = new Props();

  /**
   * Test that if the validator directory does not exist, XmlValidatorManager should still load the
   * default validator.
   */
  @Test
  public void testNoValidatorsDir() {
    final Props props = new Props(this.baseProps);

    final XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals(
        "XmlValidatorManager should contain only the default validator when no xml configuration "
            + "file is present.", manager.getValidatorsInfo().size(), 1);
    assertEquals(
        "XmlValidatorManager should contain only the default validator when no xml configuration "
            + "file is present.", manager.getValidatorsInfo().get(0),
        XmlValidatorManager.DEFAULT_VALIDATOR_KEY);
  }

  /**
   * Test that if the validator directory exists but the xml configuration file does not,
   * XmlValidatorManager only loads the default validator.
   */
  @Test
  public void testDefaultValidator() {
    final Props props = new Props(this.baseProps);
    final URL validatorUrl = Resources.getResource("project/testValidators");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());

    final XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals(
        "XmlValidatorManager should contain only the default validator when no xml configuration "
            + "file is present.", manager.getValidatorsInfo().size(), 1);
    assertEquals(
        "XmlValidatorManager should contain only the default validator when no xml configuration "
            + "file is present.", manager.getValidatorsInfo().get(0),
        XmlValidatorManager.DEFAULT_VALIDATOR_KEY);
  }

  /**
   * Test that if the xml config file specifies a validator classname that does not exist,
   * XmlValidatorManager should throw an exception.
   */
  @Test(expected = ValidatorManagerException.class)
  public void testValidatorDoesNotExist() {
    final Props props = new Props(this.baseProps);
    final URL validatorUrl = Resources.getResource("project/testValidators");
    final URL configUrl = Resources.getResource("test-conf/azkaban-validators-test1.xml");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());
    props.put(ValidatorConfigs.XML_FILE_PARAM, configUrl.getPath());

    new XmlValidatorManager(props);

  }

  /**
   * Test that if the xml config file is properly set, XmlValidatorManager loads both the default
   * validator and the one specified in the xml file. The TestValidator class specified in the xml
   * configuration file is located with the jar file inside test resource directory
   * project/testValidators.
   */
  @Test
  public void testLoadValidators() {
    final Props props = new Props(this.baseProps);
    final URL validatorUrl = Resources.getResource("project/testValidators");
    final URL configUrl = Resources.getResource("test-conf/azkaban-validators-test2.xml");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());
    props.put(ValidatorConfigs.XML_FILE_PARAM, configUrl.getPath());

    final XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals("XmlValidatorManager should contain 2 validators.",
        manager.getValidatorsInfo().size(), 2);
    assertEquals(
        "XmlValidatorManager should contain the validator specified in the xml configuration file.",
        manager.getValidatorsInfo().get(1), "Test");
  }

}
