package azkaban.project.validator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import azkaban.project.Project;
import azkaban.utils.HashUtils;
import azkaban.utils.Props;
import com.google.common.io.Resources;
import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;


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

  @Test
  public void testValidateProjectValidator() throws Exception {
    final Project project = mock(Project.class);
    final File projectDir = mock(File.class);
    final Props props = new Props();

    final ProjectValidator mockValidator = mock(ProjectValidator.class);
    final XmlValidatorManager manager = new XmlValidatorManager(props);

    // When we attempt to validate a project with our mock validator, return sampleReport
    final ValidationReport sampleReport = new ValidationReport();
    when(mockValidator.validateProject(project, projectDir, props)).thenReturn(sampleReport);

    Map<String, ProjectValidator> mockedValidators = new HashMap();
    mockedValidators.put("TEST", mockValidator);

    // This is brittle and hacky, but we're directly setting the private validators field with our own
    Field validatorsField = manager.getClass().getDeclaredField("validators");
    validatorsField.setAccessible(true);
    FieldSetter.setField(manager, validatorsField, mockedValidators);

    Map<String, ValidationReport> expectedResultingReports = new HashMap();
    expectedResultingReports.put("TEST", sampleReport);

    // Make sure we get the reports back that we expect
    assertEquals(expectedResultingReports, manager.validate(project, projectDir, props));
  }

  @Test
  public void testGetCacheKeyProjectValidatorCacheable() throws Exception {
    final Project project = mock(Project.class);
    final File projectDir = mock(File.class);
    final Props props = new Props();

    final ProjectValidatorCacheable mockValidatorCacheable = mock(ProjectValidatorCacheable.class);
    final ProjectValidator mockValidator = mock(ProjectValidator.class);
    final XmlValidatorManager manager = new XmlValidatorManager(props);

    // When we attempt to validate a project with our mock validator, return sampleReport
    final String cacheKey = "abc123";
    final String expectedResultingCacheKey = HashUtils.SHA1.getHashStr(cacheKey);
    when(mockValidatorCacheable.getCacheKey(project, projectDir, props)).thenReturn(cacheKey);

    Map<String, ProjectValidator> mockedValidators = new HashMap();
    // We add a non cacheable validator as well to ensure that it is ignored properly and no exceptions are thrown.
    mockedValidators.put("NORMAL", mockValidator);
    // Only this cacheable validator should contribute to the cacheKey
    mockedValidators.put("CACHEABLE", mockValidatorCacheable);

    // This is brittle and hacky, but we're directly setting the private validators field with our own
    Field validatorsField = manager.getClass().getDeclaredField("validators");
    validatorsField.setAccessible(true);
    FieldSetter.setField(manager, validatorsField, mockedValidators);

    // Make sure we get the cache key we expected (the SHA1 of the one cache key returned)
    assertEquals(expectedResultingCacheKey, manager.getCacheKey(project, projectDir, props));
  }
}
