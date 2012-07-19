package azkaban.user;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

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

/**
 * Xml implementation of the UserManager. Looks for the property
 * user.manager.xml.file in the azkaban properties.
 * 
 * The xml to be in the following form: <azkaban-users> <user
 * username="username" password="azkaban" roles="admin" groups="azkaban"/>
 * </azkaban-users>
 * 
 * @author rpark
 * 
 */
public class XmlUserManager implements UserManager {
	private static final Logger logger = Logger.getLogger(XmlUserManager.class.getName());

	public static final String XML_FILE_PARAM = "user.manager.xml.file";
	public static final String AZKABAN_USERS_TAG = "azkaban-users";
	public static final String USER_TAG = "user";
	public static final String ROLE_TAG = "role";
	public static final String ROLENAME_ATTR = "rolename";
	public static final String USERNAME_ATTR = "username";
	public static final String PASSWORD_ATTR = "password";
	public static final String ROLES_ATTR = "roles";
	public static final String GROUPS_ATTR = "groups";

	private String xmlPath;

	private HashMap<String, User> users;
	private HashMap<String, String> userPassword;

	/**
	 * The constructor.
	 * 
	 * @param props
	 */
	public XmlUserManager(Props props) {
		xmlPath = props.getString(XML_FILE_PARAM);

		parseXMLFile();
	}

	private void parseXMLFile() {
		File file = new File(xmlPath);
		if (!file.exists()) {
			throw new IllegalArgumentException("User xml file " + xmlPath
					+ " doesn't exist.");
		}

		HashMap<String, User> users = new HashMap<String, User>();
		HashMap<String, String> userPassword = new HashMap<String, String>();

		// Creating the document builder to parse xml.
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
				.newInstance();
		DocumentBuilder builder = null;
		try {
			builder = docBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new IllegalArgumentException(
					"Exception while parsing user xml. Document builder not created.",
					e);
		}

		Document doc = null;
		try {
			doc = builder.parse(file);
		} catch (SAXException e) {
			throw new IllegalArgumentException("Exception while parsing "
					+ xmlPath + ". Invalid XML.", e);
		} catch (IOException e) {
			throw new IllegalArgumentException("Exception while parsing "
					+ xmlPath + ". Error reading file.", e);
		}

		// Only look at first item, because we should only be seeing
		// azkaban-users tag.
		NodeList tagList = doc.getChildNodes();
		Node azkabanUsers = tagList.item(0);

		NodeList azkabanUsersList = azkabanUsers.getChildNodes();
		for (int i = 0; i < azkabanUsersList.getLength(); ++i) {
			Node node = azkabanUsersList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				if (node.getNodeName().equals(USER_TAG)) {
					parseUserTag(node, users, userPassword);
				}
			}
		}

		// Synchronize the swap. Similarly, the gets are synchronized to this.
		synchronized (this) {
			this.users = users;
			this.userPassword = userPassword;
		}
	}

	private void parseUserTag(Node node, HashMap<String, User> users, HashMap<String, String> userPassword) {
		NamedNodeMap userAttrMap = node.getAttributes();
		Node userNameAttr = userAttrMap.getNamedItem(USERNAME_ATTR);
		if (userNameAttr == null) {
			throw new RuntimeException(
					"Error loading user. The username doesn't exist");
		}
		Node passwordAttr = userAttrMap.getNamedItem(PASSWORD_ATTR);
		if (passwordAttr == null) {
			throw new RuntimeException(
					"Error loading user. The password doesn't exist for "
							+ passwordAttr);
		}

		// Add user to the user/password map
		String username = userNameAttr.getNodeValue();
		String password = passwordAttr.getNodeValue();
		userPassword.put(username, password);
		// Add the user to the node
		User user = new User(userNameAttr.getNodeValue());
		users.put(username, user);
		logger.info("Loading user " + user.getUserId());

		Node roles = userAttrMap.getNamedItem(ROLES_ATTR);
		if (roles != null) {
			String value = roles.getNodeValue();
			String[] roleSplit = value.split("\\s*,\\s*");
			for (String role : roleSplit) {
				user.addRole(role);
			}
		}

		Node groups = userAttrMap.getNamedItem(GROUPS_ATTR);
		if (groups != null) {
			String value = groups.getNodeValue();
			String[] groupSplit = value.split("\\s*,\\s*");
			for (String group : groupSplit) {
				user.addGroup(group);
			}
		}
	}

	@Override
	public User getUser(String username, String password) throws UserManagerException {
		if (username == null || username.trim().isEmpty()) {
			throw new UserManagerException("Username is empty.");
		} else if (password == null || password.trim().isEmpty()) {
			throw new UserManagerException("Password is empty.");
		}

		// Minimize the synchronization of the get. Shouldn't matter if it
		// doesn't exist.
		String foundPassword = null;
		User user = null;
		synchronized (this) {
			foundPassword = userPassword.get(username);
			if (foundPassword != null) {
				user = users.get(username);
			}
		}

		if (foundPassword == null || !foundPassword.equals(password)) {
			throw new UserManagerException("Username/Password not found.");
		}
		// Once it gets to this point, no exception has been thrown. User
		// shoudn't be
		// null, but adding this check for if user and user/password hash tables
		// go
		// out of sync.
		if (user == null) {
			throw new UserManagerException("Internal error: User not found.");
		}
		return user;
	}
}
