package azkaban.project.validator;

import static org.junit.Assert.assertEquals;

import azkaban.utils.Props;
import com.google.common.io.Resources;
import java.net.URL;
import org.junit.Test;

public class XmlValidatorManagerTest {

  private final Props baseProps = new Props();

  /**
   * Test that no validator directory exists when there is no xml configuration.
   */
  @Test
  public void testNoValidatorsDir() {
    final Props props = new Props(this.baseProps);

    final XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals(
        "XmlValidatorManager should contain 0 validator when no xml configuration "
            + "file is present.", manager.getValidatorsInfo().size(), 0);
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
   * Test that if the xml config file is properly set, XmlValidatorManager loads the validator
   * specified in the xml file. The TestValidator class specified in the xml configuration file
   * is located with the jar file inside test resource directory project/testValidators.
   */
  @Test
  public void testLoadValidators() {
    final Props props = new Props(this.baseProps);
    final URL validatorUrl = Resources.getResource("project/testValidators");
    final URL configUrl = Resources.getResource("test-conf/azkaban-validators-test2.xml");
    props.put(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, validatorUrl.getPath());
    props.put(ValidatorConfigs.XML_FILE_PARAM, configUrl.getPath());

    final XmlValidatorManager manager = new XmlValidatorManager(props);
    assertEquals("XmlValidatorManager should contain 1 validator.",
        manager.getValidatorsInfo().size(), 1);
    assertEquals(
        "XmlValidatorManager should contain the validator specified in the xml configuration file.",
        manager.getValidatorsInfo().get(0), "Test");
  }

}
