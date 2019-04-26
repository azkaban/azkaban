package azkaban.project.validator;

import azkaban.project.Project;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger LOG = LoggerFactory.getLogger(XmlValidatorManager.class);
  public static final String VALIDATOR_TAG = "validator";
  public static final String CLASSNAME_ATTR = "classname";
  public static final String ITEM_TAG = "property";
  private static final Map<String, Long> resourceTimestamps = new HashMap<>();
  private static ValidatorClassLoader validatorLoader;
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
      LOG.warn("Validator directory " + this.validatorDirPath
          + " does not exist or is not a directory.");
    }

    // Check for updated validator JAR files
    checkResources();

    // Load the validators specified in the xml file.
    try {
      loadValidators(props, org.apache.log4j.Logger.getLogger(XmlValidatorManager.class));
    } catch (final Exception e) {
      LOG.error("Cannot load all the validators.");
      throw new ValidatorManagerException(e);
    }
  }

  private void checkResources() {
    final File validatorDir = new File(this.validatorDirPath);
    final List<URL> resources = new ArrayList<>();
    boolean reloadResources = false;
    try {
      if (validatorDir.canRead() && validatorDir.isDirectory()) {
        for (final File f : validatorDir.listFiles()) {
          if (f.getName().endsWith(".jar")) {
            resources.add(f.toURI().toURL());
            if (resourceTimestamps.get(f.getName()) == null
                || resourceTimestamps.get(f.getName()) != f.lastModified()) {
              reloadResources = true;
              LOG.info("Resource " + f.getName() + " is updated. Reload the classloader.");
              resourceTimestamps.put(f.getName(), f.lastModified());
            }
          }
        }
      }
    } catch (final MalformedURLException e) {
      throw new ValidatorManagerException(e);
    }

    if (reloadResources) {
      if (validatorLoader != null) {
        try {
          // Since we cannot use Java 7 feature inside Azkaban (....), we need a customized class loader
          // that does the close for us.
          validatorLoader.close();
        } catch (final ValidatorManagerException e) {
          LOG.error("Cannot reload validator classloader because failure "
              + "to close the validator classloader.", e);
          // We do not throw the ValidatorManagerException because we do not want to crash Azkaban at runtime.
        }
      }
      validatorLoader = new ValidatorClassLoader(resources.toArray(new URL[resources.size()]));
    }
  }

  /**
   * Instances of the validators are created here rather than in the constructors. This is because
   * some validators might need to maintain project-specific states. By instantiating the validators
   * here, it ensures that the validator objects are project-specific, rather than global.
   *
   * {@inheritDoc}
   *
   * @see azkaban.project.validator.ValidatorManager#loadValidators(azkaban.utils.Props,
   * org.apache.log4j.Logger)
   */
  @Override
  public void loadValidators(final Props props, final org.apache.log4j.Logger log) {
    this.validators = new LinkedHashMap<>();
    if (!props.containsKey(ValidatorConfigs.XML_FILE_PARAM)) {
      LOG.warn(
          "Azkaban properties file does not contain the key " + ValidatorConfigs.XML_FILE_PARAM);
      return;
    }
    final String xmlPath = props.get(ValidatorConfigs.XML_FILE_PARAM);
    final File file = new File(xmlPath);
    if (!file.exists()) {
      LOG.error("Azkaban validator configuration file " + xmlPath + " does not exist.");
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

  private void parseValidatorTag(final Node node, final Props props,
      final org.apache.log4j.Logger log) {
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
      final Class<? extends ProjectValidator> validatorClass =
          (Class<? extends ProjectValidator>) validatorLoader.loadClass(className);
      final Constructor<?> validatorConstructor =
          validatorClass.getConstructor(org.apache.log4j.Logger.class);
      final ProjectValidator validator = (ProjectValidator) validatorConstructor.newInstance(log);
      validator.initialize(props);
      this.validators.put(validator.getValidatorName(), validator);
      LOG.info("Added validator " + className + " to list of validators.");
    } catch (final Exception e) {
      LOG.error("Could not instantiate ProjectValidator " + className);
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

  @Override
  public Map<String, ValidationReport> validate(final Project project, final File projectDir) {
    final Map<String, ValidationReport> reports = new LinkedHashMap<>();
    for (final Entry<String, ProjectValidator> validator : this.validators.entrySet()) {
      reports.put(validator.getKey(), validator.getValue().validateProject(project, projectDir));
      LOG.info("Validation status of validator " + validator.getKey() + " is "
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
