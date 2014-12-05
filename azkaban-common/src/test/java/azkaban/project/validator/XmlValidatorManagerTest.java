package azkaban.project.validator;

import static org.junit.Assert.*;

import java.net.URL;

import org.junit.Test;

import com.google.common.io.Resources;

import azkaban.utils.Props;

public class XmlValidatorManagerTest {
  private Props baseProps = new Props();

  /**
   * Test that if the validator directory does not exist, XmlValidatorManager
   * should still load the default validator.
   */
  @Test
  public void testNoValidatorsDir() {
    Props props = new Props(baseProps);

    XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals("XmlValidatorManager should contain only the default validator when no xml configuration "
        + "file is present.", manager.getValidatorsInfo().size(), 1);
    assertEquals("XmlValidatorManager should contain only the default validator when no xml configuration "
        + "file is present.", manager.getValidatorsInfo().get(0), XmlValidatorManager.DEFAULT_VALIDATOR_KEY);
  }

  /**
   * Test that if the validator directory exists but the xml configuration file does not,
   * XmlValidatorManager only loads the default validator.
   */
  @Test
  public void testDefaultValidator() {
    Props props = new Props(baseProps);
    URL validatorUrl = Resources.getResource("project/testValidators");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());

    XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals("XmlValidatorManager should contain only the default validator when no xml configuration "
        + "file is present.", manager.getValidatorsInfo().size(), 1);
    assertEquals("XmlValidatorManager should contain only the default validator when no xml configuration "
        + "file is present.", manager.getValidatorsInfo().get(0), XmlValidatorManager.DEFAULT_VALIDATOR_KEY);
  }

  /**
   * Test that if the xml config file specifies a validator classname that does not exist,
   * XmlValidatorManager should throw an exception.
   */
  @Test(expected=ValidatorManagerException.class)
  public void testValidatorDoesNotExist() {
    Props props = new Props(baseProps);
    URL validatorUrl = Resources.getResource("project/testValidators");
    URL configUrl = Resources.getResource("test-conf/azkaban-validators-test1.xml");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());
    props.put(ValidatorConfigs.XML_FILE_PARAM, configUrl.getPath());

    new XmlValidatorManager(props);

  }

  /**
   * Test that if the xml config file is properly set, XmlValidatorManager loads both the default
   * validator and the one specified in the xml file. The TestValidator class specified in the xml
   * configuration file is located with the jar file inside test resource directory project/testValidators.
   */
  @Test
  public void testLoadValidators() {
    Props props = new Props(baseProps);
    URL validatorUrl = Resources.getResource("project/testValidators");
    URL configUrl = Resources.getResource("test-conf/azkaban-validators-test2.xml");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());
    props.put(ValidatorConfigs.XML_FILE_PARAM, configUrl.getPath());

    XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals("XmlValidatorManager should contain 2 validators.", manager.getValidatorsInfo().size(), 2);
    assertEquals("XmlValidatorManager should contain the validator specified in the xml configuration file.",
        manager.getValidatorsInfo().get(1), "Test");
  }

}
