/*
 * Copyright 2011 LinkedIn, Inc
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

package azkaban.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.utils.Props;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class HadoopSecurityManager_H_1_0_2 extends HadoopSecurityManager {

	private static UserGroupInformation loginUser = null;
	private static final Logger logger = Logger.getLogger(HadoopSecurityManager_H_1_0_2.class);
	private static Configuration conf;
	private Props props = null;
	
	private String keytabLocation;
	private String proxyUser;
	private static boolean shouldProxy = false;
	private static boolean securityEnabled = false;

	public HadoopSecurityManager_H_1_0_2(Props props) throws MalformedURLException, SecurityManagerException {
		
		this.props = props;
		
		ClassLoader cl;

		String hadoopHome = System.getenv("HADOOP_HOME");
		String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

		if (hadoopConfDir != null) {
			logger.info("Using hadoop config found in " + hadoopConfDir);
			cl = new URLClassLoader(new URL[] { new File(hadoopConfDir)
					.toURI().toURL() }, getClass().getClassLoader());
		} else if (hadoopHome != null) {
			logger.info("Using hadoop config found in " + hadoopHome);
			cl = new URLClassLoader(
					new URL[] { new File(hadoopHome, "conf").toURI().toURL() },
					getClass().getClassLoader());
		} else {
			logger.info("HADOOP_HOME not set, using default hadoop config.");
			cl = getClass().getClassLoader();
		}
		conf = new Configuration();
		conf.setClassLoader(cl);
		
		logger.info("Hadoop Security Manager Initiated");
		logger.info("hadoop.security.authentication set to " + conf.get("hadoop.security.authentication"));
		logger.info("hadoop.security.authorization set to " + conf.get("hadoop.security.authorization"));
		logger.info("DFS name " + conf.get("fs.default.name"));
		
		UserGroupInformation.setConfiguration(conf);
		
		securityEnabled = UserGroupInformation.isSecurityEnabled();
		if(securityEnabled) {
			logger.info("The Hadoop cluster security enabled is " + UserGroupInformation.isSecurityEnabled());
			keytabLocation = props.getString(PROXY_KEYTAB_LOCATION);
			proxyUser = props.getString(PROXY_USER);
			if(keytabLocation == null || proxyUser == null) {
				throw new SecurityManagerException("Azkaban Keytab info missing on secured hadoop cluster.");
			}
			shouldProxy = true;
		}
		
	}
	
	/**
	 * Create a proxied user based on the explicit user name, taking other parameters
	 * necessary from properties file.
	 * @throws IOException 
	 */
	@Override
	public synchronized UserGroupInformation getProxiedUser(String toProxy) throws SecurityManagerException {
		// don't do privileged actions in case the hadoop is not secured.
		if(!isHadoopSecurityEnabled()) {
			logger.error("Can't get proxy user with unsecured cluster");
			return null;
		}
		
		if(toProxy == null) {
			throw new SecurityManagerException("toProxy can't be null");
		}
		
		try {
			if (loginUser == null) {
				logger.info("No login user. Creating login user");
				String keytab = verifySecureProperty(props, PROXY_KEYTAB_LOCATION);
				String proxyUser = verifySecureProperty(props, PROXY_USER);
				UserGroupInformation.loginUserFromKeytab(proxyUser, keytab);
				loginUser = UserGroupInformation.getLoginUser();
				logger.info("Logged in with user " + loginUser);
			} else {
				logger.info("loginUser (" + loginUser + ") already created, refreshing tgt.");
				loginUser.checkTGTAndReloginFromKeytab();
			}
		}
		catch (IOException e) {
			throw new SecurityManagerException("Failed to get Proxy user ", e);
		}

		return UserGroupInformation.createProxyUser(toProxy, loginUser);
	}

	/**
	* Create a proxied user, taking all parameters, including which user to proxy
	* from provided Properties.
	*/
	@Override
	public UserGroupInformation getProxiedUser(Props userProp) throws SecurityManagerException {
		String toProxy = verifySecureProperty(userProp, TO_PROXY);
		UserGroupInformation user = getProxiedUser(toProxy);
		if(user == null) throw new SecurityManagerException("Proxy as any user in unsecured grid is not supported!");
		return user;
	}

	public String verifySecureProperty(Props props, String s) throws SecurityManagerException {
		String value = props.getString(s);
		if(value == null) {
			throw new SecurityManagerException(s + " not set in properties.");
		}
		logger.info("Secure proxy configuration: Property " + s + " = " + value);
		return value;
	}
	
	@Override
	public FileSystem getFSAsUser(String user) throws SecurityManagerException {
		FileSystem fs;
		try {
			UserGroupInformation ugi = getProxiedUser(user);
			
			if (ugi != null) {
				fs = ugi.doAs(new PrivilegedAction<FileSystem>(){

					@Override	
					public FileSystem run() {
						try {
							return FileSystem.get(conf);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				});
			}
			else {
				fs = FileSystem.get(conf);
			}
		}
		catch (Exception e)
		{
			throw new SecurityManagerException("Failed to get FileSystem. ", e);
		}
		return fs;
	}

	public boolean shouldProxy() {
		return shouldProxy;
	}

	@Override
	public boolean isHadoopSecurityEnabled()
	{
		return securityEnabled;
	}
	
	/*
	 * Gets hadoop tokens for a user to run mapred/pig jobs on a secured cluster
	 */
	@Override
	public synchronized void prefetchToken(final File tokenFile, final String userToProxy) throws SecurityManagerException {
		
		//final Configuration conf = new Configuration(this.conf);
		logger.info("Creating hadoop tokens for "+userToProxy);

		try{
			getProxiedUser(userToProxy).doAs(
			//UserGroupInformation.getCurrentUser().doAs(
				new PrivilegedExceptionAction<Void>() {
						@Override
						public Void run() throws Exception {
							getToken(userToProxy);
							return null;
						}
						
						private void getToken(String userToProxy) throws InterruptedException,IOException, SecurityManagerException {
							logger.info("Pre-fetching tokens");
			
							logger.info("Pre-fetching DFS token");
							FileSystem fs = FileSystem.get(conf);
							logger.info("Getting DFS token from " + fs.getCanonicalServiceName() + fs.getName());
							Token<?> fsToken = fs.getDelegationToken(userToProxy);
							if(fsToken == null) {
								logger.error("Failed to fetch DFS token for ");
								throw new SecurityManagerException("Failed to fetch DFS token for " + userToProxy);
							}
							logger.info("Created DFS token: " + fsToken.toString());
								
							Job job = new Job(conf,"totally phony, extremely fake, not real job");			
							JobConf jc = new JobConf(conf);
							JobClient jobClient = new JobClient(jc);
							logger.info("Pre-fetching JT token: Got new JobClient: " + jc);
	
							Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(new Text("hi"));
							if(mrdt == null) {
								logger.error("Failed to fetch JT token for ");
								throw new SecurityManagerException("Failed to fetch JT token for " + userToProxy);
							}
							logger.info("Created JT token: " + mrdt.toString());
								
							job.getCredentials().addToken(new Text("howdy"), mrdt);
							job.getCredentials().addToken(fsToken.getService(), fsToken);
				
					//			File temp = File.createTempFile("mr-azkaban", ".token");
					//			temp.deleteOnExit();
					
							FileOutputStream fos = null;
							DataOutputStream dos = null;
							try {
								fos = new FileOutputStream(tokenFile);
								dos = new DataOutputStream(fos);
								job.getCredentials().writeTokenStorageToStream(	dos);
							} finally {
								if (dos != null) {
									dos.close();
								}
								if (fos != null) {
									fos.close();
								}
							}
							logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());
						}
				}
			);
		}
		catch(Exception e) {
			throw new SecurityManagerException("Failed to get hadoop tokens!", e);
		}
	}
	
}
