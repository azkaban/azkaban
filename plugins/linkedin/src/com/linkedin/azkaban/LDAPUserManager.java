package com.linkedin.azkaban;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchResult;

import org.apache.log4j.Logger;

import azkaban.utils.Props;
import azkaban.webapp.user.User;
import azkaban.webapp.user.UserManager;

public class LDAPUserManager implements UserManager {
    private static final Logger logger = Logger.getLogger(LDAPUserManager.class);
	
	private static final String LINKEDIN_JNDI_CONTEXT_FACTORY = "linkedin.jndi.initial.context.factory";
	private static final String LINKEDIN_JNDI_PROVIDER_URL = "linkedin.jndi.provider.url";
	private static final String LINKEDIN_JNDI_SECURITY_AUTHENTICATION = "linkedin.jndi.security.authentication";
	private static final String LINKEDIN_JNDI_SECURITY_PRINCIPAL_PATTERN = "linkedin.jndi.security.principal.pattern";
	
	private static final String HADOOP_JNDI_CONTEXT_FACTORY = "hadoop.jndi.initial.context.factory";
	private static final String HADOOP_JNDI_PROVIDER_URL = "hadoop.jndi.provider.url";
	private static final String HADOOP_JNDI_SEARCH_NAME = "hadoop.jndi.search.name";

	private String linkedinJNDIProviderUrl;
	private String linkedinJNDISecurityAuthentication;
	private String linkedinJNDIInitialContextFactory;
	private String linkedinJNDISecurityPrincipalPattern;
	private String hadoopJNDIProviderUrl;
	private String hadoopJNDIInitialContextFactory;
	private String hadoopJNDISearchName;
	
	public LDAPUserManager() {
	}
	
	@Override
	public User getUser(String username, String password) {
		User user = authenticateWithLinkedin(username, password);
		if (user != null && user.getError() == null) {
			this.authenticateWithHadoopJNDI(user);
		}
		
		return user;
	}
	
	@SuppressWarnings("unchecked")
	private User authenticateWithLinkedin(String username, String password) {
		String jndiSecurityPrincipal = linkedinJNDISecurityPrincipalPattern.replaceAll("\\$\\{username\\}", username);
		logger.info("Linkedin user: " + jndiSecurityPrincipal);
		
		Hashtable env = new Hashtable();
		env.put(Context.INITIAL_CONTEXT_FACTORY, linkedinJNDIInitialContextFactory);
		env.put(Context.PROVIDER_URL, linkedinJNDIProviderUrl);
		env.put(Context.SECURITY_AUTHENTICATION, linkedinJNDISecurityAuthentication);
		env.put(Context.SECURITY_PRINCIPAL, jndiSecurityPrincipal);
		env.put(Context.SECURITY_CREDENTIALS, password);

		User user = new User(username);
		DirContext ctx = null;
		try {
			ctx = new InitialDirContext(env);
		} 
		catch (NamingException e) {
			user.setError("Error logging in. Incorrect username/password.", e);
		}
		finally {
			if (ctx != null) {
				try {
					ctx.close();
				} catch (NamingException e) {
					logger.error(e);
				}
			}
		}
		
		return user;
	}
	
	@SuppressWarnings("unchecked")
	private void authenticateWithHadoopJNDI(User user) {
		logger.info("Hadoop Linkedin user: " + user.getUserId());
		Hashtable env = new Hashtable();
		env.put(Context.INITIAL_CONTEXT_FACTORY, hadoopJNDIInitialContextFactory);
		env.put(Context.PROVIDER_URL, hadoopJNDIProviderUrl);
		
		Attributes attr = new BasicAttributes();
		attr.put("memberuid", user.getUserId());
		String[] filter = {"cn"};
		
		DirContext ctx = null;
		try {
			ctx = new InitialDirContext(env);
			NamingEnumeration<SearchResult> results = ctx.search(hadoopJNDISearchName, attr, filter);
			while(results.hasMore()) {
				SearchResult result = results.next();
				logger.info(result);
				Attributes attributes = result.getAttributes();
				Attribute cnAttr = attributes.get("cn");
				String groupName = (String)cnAttr.get();
				user.addGroup(groupName);
			}
		} 
		catch (Exception e) {
			user.setError("Hadoop account not found.", e);
		}
		finally {
			if (ctx != null) {
				try {
					ctx.close();
				} catch (NamingException e) {
					logger.error(e);
				}
			}
		}
		
		logger.info(user.toString());
	}
	
	@Override
	public void init(Props props) {
		linkedinJNDIProviderUrl = props.get(LINKEDIN_JNDI_PROVIDER_URL);
		linkedinJNDIInitialContextFactory = props.getString(LINKEDIN_JNDI_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		linkedinJNDISecurityAuthentication = props.getString(LINKEDIN_JNDI_SECURITY_AUTHENTICATION, "simple");
		linkedinJNDISecurityPrincipalPattern = props.getString(LINKEDIN_JNDI_SECURITY_PRINCIPAL_PATTERN, "${username}");
		
		hadoopJNDIProviderUrl = props.get(HADOOP_JNDI_PROVIDER_URL);
		hadoopJNDIInitialContextFactory = props.getString(HADOOP_JNDI_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		hadoopJNDISearchName = props.getString(HADOOP_JNDI_SEARCH_NAME);
	}
}
