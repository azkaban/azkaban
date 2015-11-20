package azkaban.project.validator;

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

import azkaban.project.Project;
import azkaban.project.DirectoryFlowLoader;
import azkaban.utils.Props;

/**
 * Xml implementation of the ValidatorManager. Looks for the property
 * project.validators.xml.file in the azkaban properties.
 *
 * The xml to be in the following form:
 * <azkaban-validators>
 *   <validator classname="validator class name">
 *     <!-- optional configurations for each individual validator -->
 *     <property key="validator property key" value="validator property value" />
 *     ...
 *   </validator>
 * </azkaban-validators>
 */
public class XmlValidatorManager implements ValidatorManager {
  private static final Logger logger = Logger.getLogger(XmlValidatorManager.class);

  public static final String AZKABAN_VALIDATOR_TAG = "azkaban-validators";
  public static final String VALIDATOR_TAG = "validator";
  public static final String CLASSNAME_ATTR = "classname";
  public static final String ITEM_TAG = "property";
  public static final String DEFAULT_VALIDATOR_KEY = "Directory Flow";

  private static Map<String, Long> resourceTimestamps = new HashMap<String, Long>();
  private static ValidatorClassLoader validatorLoader;

  private Map<String, ProjectValidator> validators;
  private String validatorDirPath;

  /**
   * Load the validator plugins from the validator directory (default being validators/) into
   * the validator ClassLoader. This enables creating instances of these validators in the
   * loadValidators() method.
   *
   * @param props
   */
  public XmlValidatorManager(Props props) {
    validatorDirPath = props.getString(ValidatorConfigs.VALIDATOR_PLUGIN_DIR, ValidatorConfigs.DEFAULT_VALIDATOR_DIR);
    File validatorDir = new File(validatorDirPath);
    if (!validatorDir.canRead() || !validatorDir.isDirectory()) {
      logger.warn("Validator directory " + validatorDirPath
          + " does not exist or is not a directory.");
    }

    // Check for updated validator JAR files
    checkResources();

    // Load the validators specified in the xml file.
    try {
      loadValidators(props, logger);
    } catch (Exception e) {
      logger.error("Cannot load all the validators.");
      throw new ValidatorManagerException(e);
    }
  }

  private void checkResources() {
    File validatorDir = new File(validatorDirPath);
    List<URL> resources = new ArrayList<URL>();
    boolean reloadResources = false;
    try {
      if (validatorDir.canRead() && validatorDir.isDirectory()) {
        for (File f : validatorDir.listFiles()) {
          if (f.getName().endsWith(".jar")) {
            resources.add(f.toURI().toURL());
            if (resourceTimestamps.get(f.getName()) == null
                || resourceTimestamps.get(f.getName()) != f.lastModified()) {
              reloadResources = true;
              logger.info("Resource " + f.getName() + " is updated. Reload the classloader.");
              resourceTimestamps.put(f.getName(), f.lastModified());
            }
          }
        }
      }
    } catch (MalformedURLException e) {
      throw new ValidatorManagerException(e);
    }

    if (reloadResources) {
      if (validatorLoader != null) {
        try {
        // Since we cannot use Java 7 feature inside Azkaban (....), we need a customized class loader
        // that does the close for us.
          validatorLoader.close();
        } catch (ValidatorManagerException e) {
          logger.error("Cannot reload validator classloader because failure "
              + "to close the validator classloader.", e);
          // We do not throw the ValidatorManagerException because we do not want to crash Azkaban at runtime.
        }
      }
      validatorLoader = new ValidatorClassLoader(resources.toArray(new URL[resources.size()]));
    }
  }

  /**
   * Instances of the validators are created here rather than in the constructors. This is because
   * some validators might need to maintain project-specific states, such as {@link DirectoryFlowLoader}.
   * By instantiating the validators here, it ensures that the validator objects are project-specific,
   * rather than global.
   *
   * {@inheritDoc}
   * @see azkaban.project.validator.ValidatorManager#loadValidators(azkaban.utils.Props, org.apache.log4j.Logger)
   */
  @Override
  public void loadValidators(Props props, Logger log) {
    validators = new LinkedHashMap<String, ProjectValidator>();
    // Add the default validator
    DirectoryFlowLoader flowLoader = new DirectoryFlowLoader(props, log);
    validators.put(flowLoader.getValidatorName(), flowLoader);

    if (!props.containsKey(ValidatorConfigs.XML_FILE_PARAM)) {
      logger.warn("Azkaban properties file does not contain the key " + ValidatorConfigs.XML_FILE_PARAM);
      return;
    }
    String xmlPath = props.get(ValidatorConfigs.XML_FILE_PARAM);
    File file = new File(xmlPath);
    if (!file.exists()) {
      logger.error("Azkaban validator configuration file " + xmlPath + " does not exist.");
      return;
    }

    // Creating the document builder to parse xml.
    DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try {
      builder = docBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new ValidatorManagerException(
          "Exception while parsing validator xml. Document builder not created.", e);
    }

    Document doc = null;
    try {
      doc = builder.parse(file);
    } catch (SAXException e) {
      throw new ValidatorManagerException("Exception while parsing " + xmlPath
          + ". Invalid XML.", e);
    } catch (IOException e) {
      throw new ValidatorManagerException("Exception while parsing " + xmlPath
          + ". Error reading file.", e);
    }

    NodeList tagList = doc.getChildNodes();
    Node azkabanValidators = tagList.item(0);

    NodeList azkabanValidatorsList = azkabanValidators.getChildNodes();
    for (int i = 0; i < azkabanValidatorsList.getLength(); ++i) {
      Node node = azkabanValidatorsList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        if (node.getNodeName().equals(VALIDATOR_TAG)) {
          parseValidatorTag(node, props, log);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void parseValidatorTag(Node node, Props props, Logger log) {
    NamedNodeMap validatorAttrMap = node.getAttributes();
    Node classNameAttr = validatorAttrMap.getNamedItem(CLASSNAME_ATTR);
    if (classNameAttr == null) {
      throw new ValidatorManagerException(
          "Error loading validator. The validator 'classname' attribute doesn't exist");
    }

    NodeList keyValueItemsList = node.getChildNodes();
    for (int i = 0; i < keyValueItemsList.getLength(); i++) {
      Node keyValuePair = keyValueItemsList.item(i);
      if (keyValuePair.getNodeName().equals(ITEM_TAG)) {
        parseItemTag(keyValuePair, props);
      }
    }
    String className = classNameAttr.getNodeValue();
    try {
      Class<? extends ProjectValidator> validatorClass =
          (Class<? extends ProjectValidator>)validatorLoader.loadClass(className);
      Constructor<?> validatorConstructor =
          validatorClass.getConstructor(Logger.class);
      ProjectValidator validator = (ProjectValidator) validatorConstructor.newInstance(log);
      validator.initialize(props);
      validators.put(validator.getValidatorName(), validator);
      logger.info("Added validator " + className + " to list of validators.");
    } catch (Exception e) {
      logger.error("Could not instantiate ProjectValidator " + className);
      throw new ValidatorManagerException(e);
    }
  }

  private void parseItemTag(Node node, Props props) {
    NamedNodeMap keyValueMap = node.getAttributes();
    Node keyAttr = keyValueMap.getNamedItem("key");
    Node valueAttr = keyValueMap.getNamedItem("value");
    if (keyAttr == null || valueAttr == null) {
      throw new ValidatorManagerException("Error loading validator key/value "
          + "pair. The 'key' or 'value' attribute doesn't exist");
    }
    props.put(keyAttr.getNodeValue(), valueAttr.getNodeValue());
  }

  @Override
  public Map<String, ValidationReport> validate(Project project, File projectDir) {
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (Entry<String, ProjectValidator> validator : validators.entrySet()) {
      reports.put(validator.getKey(), validator.getValue().validateProject(project, projectDir));
      logger.info("Validation status of validator " + validator.getKey() + " is "
          + reports.get(validator.getKey()).getStatus());
    }
    return reports;
  }

  @Override
  public ProjectValidator getDefaultValidator() {
    return validators.get(DEFAULT_VALIDATOR_KEY);
  }

  @Override
  public List<String> getValidatorsInfo() {
    List<String> info = new ArrayList<String>();
    for (String key : validators.keySet()) {
      info.add(key);
    }
    return info;
  }

}
