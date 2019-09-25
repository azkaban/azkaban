package azkaban.project.validator;

import azkaban.project.Project;
import azkaban.utils.HashUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Xml implementation of the ValidatorManager.
 *
 * <p>Looks for the property project.validators.xml.file in the azkaban properties.
 *
 * <p>The xml to be in the following form:
 * <pre>{@code
 * <azkaban-validators>
 *   <validator classname="validator class name">
 *     <!-- optional configurations for each individual validator -->
 *      <property key="validator property key" value="validator property value" />
 *       ...
 *   </validator>
 * </azkaban-validators>
 * }</pre>
 */
public class XmlValidatorManager implements ValidatorManager {

  public static final String VALIDATOR_TAG = "validator";
  public static final String CLASSNAME_ATTR = "classname";
  public static final String ITEM_TAG = "property";
  private static final Logger logger = Logger.getLogger(XmlValidatorManager.class);
  private ValidatorClassLoader validatorLoader;
  private final String validatorDirPath;
  private Map<String, ProjectValidator> validators;

  /**
   * Load the validator plugins from the validator directory (default being validators/) into the
   * validator ClassLoader. This enables creating instances of these validators in the
   * loadValidators() method.
   */
  // Todo jamiesjc: guicify XmlValidatorManager class
  public XmlValidatorManager(final Props props) {
    this.validatorDirPath = props
        .getString(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, ValidatorConfigs.DEFAULT_VALIDATOR_DIR);
    final File validatorDir = new File(this.validatorDirPath);
    if (!validatorDir.canRead() || !validatorDir.isDirectory()) {
      logger.warn("Validator directory " + this.validatorDirPath
          + " does not exist or is not a directory.");
    }

    // Initialize the class loader.
    initClassLoader();

    // Load the validators specified in the xml file.
    try {
      loadValidators(props, logger);
    } catch (final Exception e) {
      logger.error("Cannot load all the validators.");
      throw new ValidatorManagerException(e);
    }
  }

  private void initClassLoader() {
    final File validatorDir = new File(this.validatorDirPath);
    final List<URL> resources = new ArrayList<>();
    try {
      if (validatorDir.canRead() && validatorDir.isDirectory()) {
        for (final File f : validatorDir.listFiles()) {
          if (f.getName().endsWith(".jar")) {
            resources.add(f.toURI().toURL());
          }
        }
      }
    } catch (final MalformedURLException e) {
      throw new ValidatorManagerException(e);
    }

    validatorLoader = new ValidatorClassLoader(resources.toArray(new URL[resources.size()]));
  }

  /**
   * Creates instances of the validators, passing in any props that are global and not project specific.
   * These validator instances are global and will be used for all projects.
   *
   * {@inheritDoc}
   *
   * @see azkaban.project.validator.ValidatorManager#loadValidators(azkaban.utils.Props,
   * org.apache.log4j.Logger)
   */
  @Override
  public void loadValidators(final Props props, final Logger log) {
    this.validators = new LinkedHashMap<>();
    if (!props.containsKey(ValidatorConfigs.XML_FILE_PARAM)) {
      logger.warn(
          "Azkaban properties file does not contain the key " + ValidatorConfigs.XML_FILE_PARAM);
      return;
    }
    final String xmlPath = props.get(ValidatorConfigs.XML_FILE_PARAM);
    final File file = new File(xmlPath);
    if (!file.exists()) {
      logger.error("Azkaban validator configuration file " + xmlPath + " does not exist.");
      return;
    }

    // Creating the document builder to parse xml.
    final DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try {
      builder = docBuilderFactory.newDocumentBuilder();
    } catch (final ParserConfigurationException e) {
      throw new ValidatorManagerException(
          "Exception while parsing validator xml. Document builder not created.", e);
    }

    Document doc = null;
    try {
      doc = builder.parse(file);
    } catch (final SAXException e) {
      throw new ValidatorManagerException("Exception while parsing " + xmlPath
          + ". Invalid XML.", e);
    } catch (final IOException e) {
      throw new ValidatorManagerException("Exception while parsing " + xmlPath
          + ". Error reading file.", e);
    }

    final NodeList tagList = doc.getChildNodes();
    final Node azkabanValidators = tagList.item(0);

    final NodeList azkabanValidatorsList = azkabanValidators.getChildNodes();
    for (int i = 0; i < azkabanValidatorsList.getLength(); ++i) {
      final Node node = azkabanValidatorsList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        if (node.getNodeName().equals(VALIDATOR_TAG)) {
          parseValidatorTag(node, props, log);
        }
      }
    }
  }

  private void parseValidatorTag(final Node node, final Props props, final Logger log) {
    final NamedNodeMap validatorAttrMap = node.getAttributes();
    final Node classNameAttr = validatorAttrMap.getNamedItem(CLASSNAME_ATTR);
    if (classNameAttr == null) {
      throw new ValidatorManagerException(
          "Error loading validator. The validator 'classname' attribute doesn't exist");
    }

    final NodeList keyValueItemsList = node.getChildNodes();
    for (int i = 0; i < keyValueItemsList.getLength(); i++) {
      final Node keyValuePair = keyValueItemsList.item(i);
      if (keyValuePair.getNodeName().equals(ITEM_TAG)) {
        parseItemTag(keyValuePair, props);
      }
    }
    final String className = classNameAttr.getNodeValue();
    try {
      // Attempt to instantiate original ProjectValidator
      final Class<? extends ProjectValidator> validatorClass =
          (Class<? extends ProjectValidator>) validatorLoader.loadClass(className);
      final Constructor<?> validatorConstructor =
          validatorClass.getConstructor(Logger.class);
      final ProjectValidator validator = (ProjectValidator) validatorConstructor.newInstance(log);
      validator.initialize(props);
      this.validators.put(validator.getValidatorName(), validator);
      logger.info("Added validator " + className + " to list of validators.");
    } catch (final Exception e) {
      logger.error("Could not instantiate ProjectValidator " + className);
      throw new ValidatorManagerException(e);
    }
  }

  private void parseItemTag(final Node node, final Props props) {
    final NamedNodeMap keyValueMap = node.getAttributes();
    final Node keyAttr = keyValueMap.getNamedItem("key");
    final Node valueAttr = keyValueMap.getNamedItem("value");
    if (keyAttr == null || valueAttr == null) {
      throw new ValidatorManagerException("Error loading validator key/value "
          + "pair. The 'key' or 'value' attribute doesn't exist");
    }
    props.put(keyAttr.getNodeValue(), valueAttr.getNodeValue());
  }

  /**
   * Gets a SHA1 hash of the combined cache keys for all loaded validators.
   *
   * @see azkaban.project.validator.ProjectValidatorCacheable#getCacheKey(azkaban.project.Project, java.io.File,
   * azkaban.utils.Props)
   */
  @Override
  public String getCacheKey(Project project, File projectDir, Props props) {
    if (props == null) {
      props = new Props();
    }

    StringBuilder compoundedKey = new StringBuilder();
    for (final Entry<String, ProjectValidator> validator : this.validators.entrySet()) {
      try {
        // Attempt to cast to ProjectValidatorCacheable
        ProjectValidatorCacheable cacheableValidator = (ProjectValidatorCacheable) validator.getValue();
        compoundedKey.append(cacheableValidator.getCacheKey(project, projectDir, props));
      } catch (ClassCastException e) {
        // Swallow this error - the validator must not have been a cacheable validator
      }
    }
    return HashUtils.SHA1.getHashStr(compoundedKey.toString());
  }

  /**
   * Validates the project with all loaded validators.
   *
   * @see azkaban.project.validator.ProjectValidator#validateProject(azkaban.project.Project, java.io.File,
   * azkaban.utils.Props)
   */
  @Override
  public Map<String, ValidationReport> validate(Project project, File projectDir, Props additionalProps) {
    if (additionalProps == null) {
      additionalProps = new Props();
    }

    final Map<String, ValidationReport> reports = new LinkedHashMap<>();
    for (final Entry<String, ProjectValidator> validator : this.validators.entrySet()) {
      reports.put(validator.getKey(), validator.getValue().validateProject(project, projectDir, additionalProps));
      logger.info("Validation status of validator " + validator.getKey() + " is "
          + reports.get(validator.getKey()).getStatus());
    }
    return reports;
  }

  @Override
  public List<String> getValidatorsInfo() {
    final List<String> info = new ArrayList<>();
    for (final String key : this.validators.keySet()) {
      info.add(key);
    }
    return info;
  }

}
