package azkaban.project.validator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
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

import azkaban.utils.Props;

public class XmlValidatorManager implements ValidatorManager {
  private static final Logger logger = Logger.getLogger(XmlValidatorManager.class
      .getName());

  public static final String XML_FILE_PARAM = "project.validators.xml.file";
  public static final String AZKABAN_VALIDATOR_TAG = "azkaban-validators";
  public static final String VALIDATOR_TAG = "validator";
  public static final String CLASSNAME_ATTR = "classname";

  private Map<String, ProjectValidator> validators;

  public XmlValidatorManager(Props props) {
    validators = new LinkedHashMap<String, ProjectValidator>();
    loadValidators(props);
  }

  @Override
  public void loadValidators(Props props) {
    String xmlPath = props.get(XML_FILE_PARAM);
    File file = new File(xmlPath);
    if (!file.exists()) {
      throw new IllegalArgumentException("Validator xml file " + xmlPath
          + " doesn't exist.");
    }

    // Creating the document builder to parse xml.
    DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try {
      builder = docBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new IllegalArgumentException(
          "Exception while parsing user xml. Document builder not created.", e);
    }

    Document doc = null;
    try {
      doc = builder.parse(file);
    } catch (SAXException e) {
      throw new IllegalArgumentException("Exception while parsing " + xmlPath
          + ". Invalid XML.", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Exception while parsing " + xmlPath
          + ". Error reading file.", e);
    }

    NodeList tagList = doc.getChildNodes();
    Node azkabanValidators = tagList.item(0);

    NodeList azkabanValidatorsList = azkabanValidators.getChildNodes();
    for (int i = 0; i < azkabanValidatorsList.getLength(); ++i) {
      Node node = azkabanValidatorsList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        if (node.getNodeName().equals(VALIDATOR_TAG)) {
          parseValidatorTag(node, props);
        }
      }
    }
  }

  private void parseValidatorTag(Node node, Props props) {
    NamedNodeMap validatorAttrMap = node.getAttributes();
    Node classNameAttr = validatorAttrMap.getNamedItem(CLASSNAME_ATTR);
    if (classNameAttr == null) {
      throw new RuntimeException(
          "Error loading validator. The validator 'classname' attribute doesn't exist");
    }

    String className = classNameAttr.getNodeValue();
    try {
      Class<?> validatorClass = Class.forName(className);
      Constructor<?> validatorConstructor =
          validatorClass.getConstructor();
      ProjectValidator validator = (ProjectValidator) validatorConstructor.newInstance();
      validator.initialize(props.toProperties());
      validators.put(validator.getValidatorInfo(), validator);
      logger.info("Added validator " + validator.getClass().getCanonicalName() + " to list of validators.");
    } catch (Exception e) {
      logger.error("Could not instantiate ProjectValidator " + className);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, ValidationReport> validate(File projectDir) {
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (Entry<String, ProjectValidator> validator : validators.entrySet()) {
      reports.put(validator.getKey(), validator.getValue().validateProject(projectDir));
    }
    return reports;
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
