package azkaban.project.validator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
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

import azkaban.utils.DirectoryFlowLoader;
import azkaban.utils.Props;

public class XmlValidatorManager implements ValidatorManager {
  private static final Logger logger = Logger.getLogger(XmlValidatorManager.class
      .getName());

  public static final String DEFAULT_VALIDATOR_DIR = "validators";
  public static final String VALIDATOR_PLUGIN_DIR = "project.validators.dir";
  public static final String XML_FILE_PARAM = "project.validators.xml.file";
  public static final String AZKABAN_VALIDATOR_TAG = "azkaban-validators";
  public static final String VALIDATOR_TAG = "validator";
  public static final String CLASSNAME_ATTR = "classname";
  public static final String ITEM_TAG = "item";
  public static final String DEFAULT_VALIDATOR_KEY = "Directory Flow";

  private Map<String, ProjectValidator> validators;
  private ClassLoader validatorLoader;

  public XmlValidatorManager(Props props) {
    String validatorDirPath = props.getString(VALIDATOR_PLUGIN_DIR, DEFAULT_VALIDATOR_DIR);
    File validatorDir = new File(validatorDirPath);
    if (!validatorDir.canRead() || !validatorDir.isDirectory()) {
      throw new ValidatorManagerException("Validator directory " + validatorDirPath
          + " does not exist or is not a direcotry.");
    }

    List<URL> resources = new ArrayList<URL>();
    try {
      logger.info("Adding validator resources.");
      for (File f : validatorDir.listFiles()) {
        if (f.getName().endsWith(".jar")) {
          resources.add(f.toURI().toURL());
          logger.debug("adding to classpath " + f.toURI().toURL());
        }
      }
    } catch (MalformedURLException e) {
      throw new ValidatorManagerException(e);
    }
    validatorLoader = new URLClassLoader(resources.toArray(new URL[resources.size()]));

    // Test loading the validators specified in the xml file.
    try {
      loadValidators(props, logger);
    } catch (Exception e) {
      logger.error("Cannot load all the validaotors.");
      throw new ValidatorManagerException(e);
    }
  }

  @Override
  public void loadValidators(Props props, Logger log) {
    validators = new LinkedHashMap<String, ProjectValidator>();
    // Add the default validator
    DirectoryFlowLoader flowLoader = new DirectoryFlowLoader(log);
    validators.put(flowLoader.getValidatorInfo(), flowLoader);

    if (!props.containsKey(XML_FILE_PARAM)) {
      return;
    }
    String xmlPath = props.get(XML_FILE_PARAM);
    File file = new File(xmlPath);
    if (!file.exists()) {
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
      validators.put(validator.getValidatorInfo(), validator);
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
  public Map<String, ValidationReport> validate(File projectDir) {
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (Entry<String, ProjectValidator> validator : validators.entrySet()) {
      reports.put(validator.getKey(), validator.getValue().validateProject(projectDir));
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
