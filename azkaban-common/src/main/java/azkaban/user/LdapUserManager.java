/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.user;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import azkaban.user.User.UserPermissions;
import azkaban.utils.Props;

/**
 * LDAP implementation of the UserManager. Requires the following properties to
 * be defined in the azkaban properties.
 * 
 * user.manager.class=azkaban.user.LdapUserManager
 * user.manager.ldap.initial.context.factory=com.sun.jndi.ldap.LdapCtxFactory
 * user.manager.ldap.provider.url=ldap://hostname:port
 * user.manager.ldap.security.authentication=simple
 * user.manager.ldap.security.principal={0}
 * 
 * user.manager.ldap.attribute.group=group
 * user.manager.ldap.groups.mapping.file=conf/groups.xml
 */

public class LdapUserManager implements UserManager {
	private static final Logger logger = Logger.getLogger(LdapUserManager.class.getName());

	private static final String INITIAL_CONTEXT_FACTORY_KEY = "user.manager.ldap.initial.context.factory";
	private static final String PROVIDER_URL_KEY = "user.manager.ldap.provider.url";
	private static final String SECURITY_AUTHENTICATION_KEY = "user.manager.ldap.security.authentication";
	private static final String SECURITY_PRINCIPAL_KEY = "user.manager.ldap.security.principal";
	private static final String LDAP_ATTRIBUTE_GROUP_KEY = "user.manager.ldap.attribute.group";
	private static final String GROUP_FILE_KEY = "user.manager.ldap.groups.mapping.file";

	private static String initialContextFactory;
	private static String providerURL;
	private static String securityAuthentication;
	private static String securityPrincipal;
	private static String attributeGroup;
	private static String groupsFile;

	private static final String GROUP_ELE_TAG = "group";
	private static final String GROUP_NAME_ATTR_TAG = "name";
	private static final String PERMISSIONS_ATTR_TAG = "permissions";

	private static Map<String, String> groupToPermissions;
	private static Map<String, Role> permissionsToRole;

	/**
	 * The constructor.
	 *
	 * @param props
	 */
	public LdapUserManager(Props props) {
		synchronized (this) {
			groupToPermissions = new HashMap<String, String>();
			permissionsToRole = new HashMap<String, Role>();

			initialContextFactory = nonNullify(props.getString(INITIAL_CONTEXT_FACTORY_KEY));
			providerURL = nonNullify(props.getString(PROVIDER_URL_KEY));
			securityAuthentication = nonNullify(props.getString(SECURITY_AUTHENTICATION_KEY));
			securityPrincipal = nonNullify(props.getString(SECURITY_PRINCIPAL_KEY));
			attributeGroup = nonNullify(props.getString(LDAP_ATTRIBUTE_GROUP_KEY));
			groupsFile = props.getString(GROUP_FILE_KEY);

			parseGroupsFile();
		}
	}

	private void parseGroupsFile() {
		if (!isValid(groupsFile)) {
			throw new IllegalArgumentException("LDAP to Azkaban groups file is not valid.");
		}
		File file = new File(groupsFile);
		if (!file.exists()) {
			throw new IllegalArgumentException("LDAP to Azkaban groups file " + groupsFile + " doesn't exist.");
		}

		Map<String, String> groupPermission = new HashMap<String, String>();
		Map<String, Role> permissionRole = new HashMap<String, Role>();

		// Creating the document builder to parse xml.
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		try {
			builder = docBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new IllegalArgumentException(
					"Exception while parsing " + groupsFile + ". Document builder not created.", e);
		}

		Document doc = null;
		try {
			doc = builder.parse(file);
		} catch (SAXException e) {
			throw new IllegalArgumentException("Exception while parsing " + groupsFile + ". Invalid XML.", e);
		} catch (IOException e) {
			throw new IllegalArgumentException("Exception while parsing " + groupsFile + ". Error reading file.", e);
		}

		// The first Node should be groups.
		NodeList groupsNodeList = doc.getChildNodes();
		Node groupsNode = groupsNodeList.item(0);

		NodeList mapping = groupsNode.getChildNodes();
		for (int i = 0; i < mapping.getLength(); ++i) {
			Node node = mapping.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				if (node.getNodeName().equals(GROUP_ELE_TAG)) {
					parseGroup(node, groupPermission, permissionRole);
				}
			}
		}

		groupToPermissions = groupPermission;
		permissionsToRole = permissionRole;
	}

	private void parseGroup(Node node, Map<String, String> groupToPermissions, Map<String, Role> permissionsToRole) {
		NamedNodeMap groupAttrMap = node.getAttributes();
		Node groupNameAttr = groupAttrMap.getNamedItem(GROUP_NAME_ATTR_TAG);
		if (groupNameAttr == null) {
			logger.error("Error loading role. The group 'name' attribute doesn't exist");
			return;
		}
		Node permissionsAttr = groupAttrMap.getNamedItem(PERMISSIONS_ATTR_TAG);
		if (permissionsAttr == null) {
			logger.error("Error loading group. The group 'permissions' attribute doesn't exist");
			return;
		}

		String groupName = groupNameAttr.getNodeValue();
		if (!isValid(groupName)) {
			logger.error("Error loading group. The group 'name' is invalid");
			return;
		}
		String permissionsString = permissionsAttr.getNodeValue();
		if (!isValid(permissionsString)) {
			logger.error("Error loading permissions for group " + groupName + ". The group 'permissions' is invalid");
			return;
		}

		String[] permissions = permissionsString.split("\\s*,\\s*");
		Arrays.sort(permissions);

		Permission permission = new Permission();
		for (String permissionString : permissions) {
			try {
				Permission.Type type = Permission.Type.valueOf(nonNullify(permissionString).toUpperCase());
				permission.addPermission(type);
			} catch (IllegalArgumentException e) {
				logger.error("Error adding type " + permissionString + ". Permission doesn't exist.", e);
			}
		}

		Role role = new Role(permission.toString(), permission);
		permissionsToRole.put(permission.toString(), role);

		if (groupToPermissions.get(groupName) != null) {
			throw new RuntimeException(
					"Error adding group " + groupName + ". The group 'permissions' has been defined more than once");
		}
		groupToPermissions.put(groupName, role.getName());
	}

	@Override
	public User getUser(String username, String password) throws UserManagerException {
		if (username == null || username.trim().isEmpty()) {
			throw new UserManagerException("Username is empty.");
		} else if (password == null || password.trim().isEmpty()) {
			throw new UserManagerException("Password is empty.");
		}
		User user = null;
		DirContext dirContext = null;
		try {
			Hashtable<String, String> environment = new Hashtable<String, String>();
			environment.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
			environment.put(Context.PROVIDER_URL, providerURL);
			environment.put(Context.SECURITY_AUTHENTICATION, securityAuthentication);
			environment.put(Context.SECURITY_PRINCIPAL, MessageFormat.format(securityPrincipal, username));
			environment.put(Context.SECURITY_CREDENTIALS, password);

			dirContext = new InitialDirContext(environment);
			user = new User(username);

			addGroupsAndPermissionsToUser(dirContext, user);

			user.setPermissions(new UserPermissions() {
				@Override
				public boolean hasPermission(String permission) {
					return true;
				}

				@Override
				public void addPermission(String permission) {
				}
			});

		} catch (AuthenticationException e) {
			logger.debug(e.getLocalizedMessage());
			throw new UserManagerException("LDAP Authentication : Invalid Credentials", e);
		} catch (Exception e) {
			logger.debug(e.getLocalizedMessage());
			throw new UserManagerException(
					"Error occurred while connecting to the LDAP server. Check the log for more details", e);
		} finally {
			try {
				if(dirContext!=null) {
					dirContext.close();
				}
			} catch (NamingException e) {
				logger.error("Error while closing the LDAP connection for the user " + username, e);
			}
		}
		return user;
	}

	@Override
	public boolean validateUser(String username) {
		return false;
	}

	@Override
	public Role getRole(String roleName) {
		return permissionsToRole.get(roleName);
	}

	@Override
	public boolean validateGroup(String group) {
		return groupToPermissions.containsKey(group);
	}

	@Override
	public boolean validateProxyUser(String proxyUser, User realUser) {
		return false;
	}

	private void addGroupsAndPermissionsToUser(DirContext dirContext, User user) throws Exception {
		String groupName, roleName, retriveAttributes[] = { attributeGroup };
		Attributes attributes = dirContext.getAttributes(MessageFormat.format(securityPrincipal, user.getUserId()),
				retriveAttributes);
		NamingEnumeration<? extends Attribute> ne = attributes.getAll();
		if (ne == null) {
			logger.error("Error while fetching LDAP group for the user " + user.getUserId());
			return;
		}
		while (ne.hasMore()) {
			Attribute attribute = ne.next();
			if (attribute == null) {
				logger.error("Error while fetching LDAP group for the user " + user.getUserId());
				return;
			}
			NamingEnumeration<?> neInner = attribute.getAll();
			if (neInner == null) {
				logger.error("Error while fetching LDAP group for the user " + user.getUserId());
				return;
			}
			while (neInner.hasMore()) {
				Object neObject = neInner.next();
				if (neObject != null) {
					String neValue = neObject.toString();
					groupName = fetchGroupCNfromDN(neValue);
					if (isValid(groupName)) {
						user.addGroup(groupName);
						roleName = groupToPermissions.get(groupName);
						if (isValid(roleName)) {
							user.addRole(roleName);
						}
					}
				}
			}
		}
	}

	private String fetchGroupCNfromDN(String groupDN) {
		String comma = ",", equals = "=", groupCN = null;
		if (isValid(groupDN)) {
			String[] commaSplitValues = groupDN.split(comma);
			if (commaSplitValues != null && commaSplitValues.length > 0) {
				for (String commaSplitValue : commaSplitValues) {
					if (isValid(commaSplitValue)) {
						String[] equalsSplitValues = commaSplitValue.split(equals);
						if (equalsSplitValues != null && equalsSplitValues.length == 2
								&& "CN".equalsIgnoreCase(nonNullify(equalsSplitValues[0]))
								&& isValid(equalsSplitValues[1])) {
							groupCN = equalsSplitValues[1];

						}
					}
				}
			}
		}
		return groupCN;
	}

	private String nonNullify(String string) {
		return (string + "").trim();
	}

	private boolean isValid(String string) {
		return string!=null && !"".equals(string.trim());
	}
}
