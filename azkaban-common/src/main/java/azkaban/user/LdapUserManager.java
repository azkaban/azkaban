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

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.log4j.Logger;

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
 * user.manager.ldap.security.principal=userDN
 */

public class LdapUserManager implements UserManager {
	private static final Logger logger = Logger.getLogger(LdapUserManager.class.getName());

	private static final String INITIAL_CONTEXT_FACTORY_KEY = "user.manager.ldap.initial.context.factory";
	private static final String PROVIDER_URL_KEY = "user.manager.ldap.provider.url";
	private static final String SECURITY_AUTHENTICATION_KEY = "user.manager.ldap.security.authentication";
	private static final String SECURITY_PRINCIPAL_KEY = "user.manager.ldap.security.principal";

	private static String INITIAL_CONTEXT_FACTORY_VALUE;
	private static String PROVIDER_URL_VALUE;
	private static String SECURITY_AUTHENTICATION_VALUE;
	private static String SECURITY_PRINCIPAL_VALUE;

	private static final String LDAP_ATTRIBUTE_GROUP_KEY = "user.manager.ldap.attribute.group";
	private static String LDAP_ATTRIBUTE_GROUP_VALUE;
	private static final String LDAP_ATTRIBUTE_ROLE_KEY = "user.manager.ldap.attribute.role";
	private static String LDAP_ATTRIBUTE_ROLE_VALUE;
	
	private Map<String, DirContext> userContext;

	/**
	 * The constructor.
	 *
	 * @param props
	 */
	public LdapUserManager(Props props) {
		userContext = new HashMap<String, DirContext>();
		INITIAL_CONTEXT_FACTORY_VALUE = props.getString(INITIAL_CONTEXT_FACTORY_KEY);
		PROVIDER_URL_VALUE = props.getString(PROVIDER_URL_KEY);
		SECURITY_AUTHENTICATION_VALUE = props.getString(SECURITY_AUTHENTICATION_KEY);
		SECURITY_PRINCIPAL_VALUE = props.getString(SECURITY_PRINCIPAL_KEY);

		LDAP_ATTRIBUTE_GROUP_VALUE = props.getString(LDAP_ATTRIBUTE_GROUP_KEY);
		LDAP_ATTRIBUTE_ROLE_VALUE = props.getString(LDAP_ATTRIBUTE_ROLE_KEY);
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
			environment.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY_VALUE);
			environment.put(Context.PROVIDER_URL, PROVIDER_URL_VALUE);
			environment.put(Context.SECURITY_AUTHENTICATION, SECURITY_AUTHENTICATION_VALUE);
			environment.put(Context.SECURITY_PRINCIPAL, MessageFormat.format(SECURITY_PRINCIPAL_VALUE, username));
			environment.put(Context.SECURITY_CREDENTIALS, password);

			dirContext = new InitialDirContext(environment);
			user = new User(username);

			// Add all the roles the group has to the user

			populateUserObject(dirContext, user);
			user.addRole("metrics");
			user.addRole("admin");

			user.setPermissions(new UserPermissions() {
				@Override
				public boolean hasPermission(String permission) {
					return true;
				}

				@Override
				public void addPermission(String permission) {
				}
			});

			userContext.put(username, dirContext);

		} catch (AuthenticationException e) {
			logger.debug(e.getLocalizedMessage());
			throw new UserManagerException("LDAP Authentication : Invalid Credentials", e);
		} catch (Exception e) {
			logger.debug(e.getLocalizedMessage());
			throw new UserManagerException(
					"Error occurred while connecting to the LDAP server. Check the log for more details", e);
		}
		return user;
	}

	@Override
	public boolean validateUser(String username) {
		return true;
	}

	@Override
	public Role getRole(String roleName) {
		return new Role("admin", new Permission(Permission.Type.ADMIN));
	}

	@Override
	public boolean validateGroup(String group) {
		logger.info("validateGroup(" + group + ")");
		// Return true. Validation should be added when groups are added to the
		// xml.
		return true;
	}

	@Override
	public boolean validateProxyUser(String proxyUser, User realUser) {
		return false;
	}

	private static void populateUserObject(DirContext dirContext, User user) throws Exception {
		NamingEnumeration<SearchResult> results = null;
		SearchControls controls = new SearchControls();
		controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
		results = dirContext.search("", "(&(cn=" + user.getUserId() + ")(objectClass=user))", controls);
		while (results.hasMore()) {
			SearchResult searchResult = (SearchResult) results.next();
			Attributes attributes = searchResult.getAttributes();
			NamingEnumeration<? extends Attribute> ne = attributes.getAll();
			while (ne.hasMore()) {
				Attribute attribute = ne.next();
				if (LDAP_ATTRIBUTE_GROUP_VALUE != null && LDAP_ATTRIBUTE_GROUP_VALUE.equals(attribute.getID())) {
					NamingEnumeration<?> neInner = attribute.getAll();
					while (neInner.hasMore()) {
						Object neObject = neInner.next();
						if (neObject != null) {
							String neValue = neObject.toString();
							if (neValue != null) {
								String[] commaSplitValues = neValue.split(",");
								if (commaSplitValues != null) {
									for (String commaSplitValue : commaSplitValues) {
										if (commaSplitValue != null) {
											String[] equalsSplitValues = commaSplitValue.split("=");
											if (equalsSplitValues != null && equalsSplitValues.length == 2
													&& "CN".equalsIgnoreCase((equalsSplitValues[0] + "").trim())
													&& !"".equals((equalsSplitValues[1] + "").trim())) {
												user.addGroup(equalsSplitValues[1]);
											}
										}
									}
								}
							}
						}
					}
				}
				if (LDAP_ATTRIBUTE_ROLE_VALUE != null && LDAP_ATTRIBUTE_ROLE_VALUE.equals(attribute.getID())) {
					NamingEnumeration<?> neInner = attribute.getAll();
					while (neInner.hasMore()) {
						Object neObject = neInner.next();
						if (neObject != null) {
							String neValue = neObject.toString();
							if (!"".equals((neValue + "").trim())) {
								user.addRole(neValue.trim().replaceAll("\\s+", "_"));
							}
						}
					}
				}
			}
		}
	}
}
