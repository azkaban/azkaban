package azkaban.project;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import azkaban.utils.Props;

/**
 * @author wkang
 * 
 * This class manages project whitelist defined in xml config file.
 * An single xml config file contains different types of whitelisted
 * projects. For additional type of whitelist, modify WhitelistType enum.
 * 
 * The xml config file should in the following format. Please note
 * the tag <MemoryCheck> is same as the defined enum MemoryCheck
 * 
 * <ProjectWhitelist>
 *  <MemoryCheck>
 *      <project projectname="project1" />
 *      <project projectname="project2" />
 *  </MemoryCheck>
 * <ProjectWhitelist>
 *
 */
public class ProjectWhitelist {
  public static final String XML_FILE_PARAM = "project.whitelist.xml.file";
  private static final String PROJECT_WHITELIST_TAG = "ProjectWhitelist";
  private static final String PROJECT_TAG = "project";
  private static final String PROJECTNAME_ATTR = "projectname";

  private static AtomicReference<Map<WhitelistType, Set<String>>> projectsWhitelisted =
          new AtomicReference<Map<WhitelistType, Set<String>>>();

  static void load(Props props) {
    String xmlFile = props.getString(XML_FILE_PARAM);
    parseXMLFile(xmlFile);
  }

  private static void parseXMLFile(String xmlFile) {
    File file = new File(xmlFile);
    if (!file.exists()) {
      throw new IllegalArgumentException("Project whitelist xml file " + xmlFile
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
          "Exception while parsing project whitelist xml. Document builder not created.", e);
    }

    Document doc = null;
    try {
      doc = builder.parse(file);
    } catch (SAXException e) {
      throw new IllegalArgumentException("Exception while parsing " + xmlFile
          + ". Invalid XML.", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Exception while parsing " + xmlFile
          + ". Error reading file.", e);
    }

    Map<WhitelistType, Set<String>> projsWhitelisted = new HashMap<WhitelistType, Set<String>>();
    NodeList tagList = doc.getChildNodes();
    if (!tagList.item(0).getNodeName().equals(PROJECT_WHITELIST_TAG)) {
      throw new RuntimeException("Cannot find tag '" +  PROJECT_WHITELIST_TAG + "' in " + xmlFile);      
    }

    NodeList whitelist = tagList.item(0).getChildNodes();
    for (int n = 0; n < whitelist.getLength(); ++n) {
      if (whitelist.item(n).getNodeType() != Node.ELEMENT_NODE) {
        continue;
      }

      String whitelistType = whitelist.item(n).getNodeName();
      Set<String> projs = new HashSet<String>();

      NodeList projectsList = whitelist.item(n).getChildNodes();
      for (int i = 0; i < projectsList.getLength(); ++i) {
        Node node = projectsList.item(i);
        if (node.getNodeType() == Node.ELEMENT_NODE) {
          if (node.getNodeName().equals(PROJECT_TAG)) {
            parseProjectTag(node, projs);
          }
        }
      }
      projsWhitelisted.put(WhitelistType.valueOf(whitelistType), projs);
    }
    projectsWhitelisted.set(projsWhitelisted);
  }

  private static void parseProjectTag(Node node, Set<String> projects) {
    NamedNodeMap projectAttrMap = node.getAttributes();
    Node projectNameAttr = projectAttrMap.getNamedItem(PROJECTNAME_ATTR);
    if (projectNameAttr == null) {
      throw new RuntimeException("Error loading project. The '" + PROJECTNAME_ATTR 
              + "' attribute doesn't exist");
    }

    String projectName = projectNameAttr.getNodeValue();
    projects.add(projectName);
  }

  public static boolean isProjectWhitelisted(String project, WhitelistType whitelistType) {
    Map<WhitelistType, Set<String>> projsWhitelisted = projectsWhitelisted.get();
    if (projsWhitelisted != null) {
      Set<String> projs = projsWhitelisted.get(whitelistType);
      if (projs != null) {
        return projs.contains(project); 
      }
    }
    return false;
  }

  /**
   * The tag in the project whitelist xml config file should be same as
   * the defined enums.
   */
  public static enum WhitelistType {
    MemoryCheck
  }
}