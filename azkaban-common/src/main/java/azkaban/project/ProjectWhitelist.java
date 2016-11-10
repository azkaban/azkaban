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
  private static final String PROJECTID_ATTR = "projectid";

  private static AtomicReference<Map<WhitelistType, Set<Integer>>> projectsWhitelisted =
          new AtomicReference<Map<WhitelistType, Set<Integer>>>();

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

    Map<WhitelistType, Set<Integer>> projsWhitelisted = new HashMap<WhitelistType, Set<Integer>>();
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
      Set<Integer> projs = new HashSet<Integer>();

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

  private static void parseProjectTag(Node node, Set<Integer> projects) {
    NamedNodeMap projectAttrMap = node.getAttributes();
    Node projectIdAttr = projectAttrMap.getNamedItem(PROJECTID_ATTR);
    if (projectIdAttr == null) {
      throw new RuntimeException("Error loading project. The '" + PROJECTID_ATTR
              + "' attribute doesn't exist");
    }

    String projectId = projectIdAttr.getNodeValue();
    projects.add(Integer.parseInt(projectId));
  }

  public static boolean isProjectWhitelisted(int project, WhitelistType whitelistType) {
    Map<WhitelistType, Set<Integer>> projsWhitelisted = projectsWhitelisted.get();
    if (projsWhitelisted != null) {
      Set<Integer> projs = projsWhitelisted.get(whitelistType);
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
    MemoryCheck,
    NumJobPerFlow
  }
}