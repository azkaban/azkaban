package azkaban.jobtype;

/*
 * Copyright 2012 LinkedIn, Inc
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
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.NoopJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.PythonJob;
import azkaban.jobExecutor.RubyJob;
import azkaban.jobExecutor.ScriptJob;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import azkaban.jobExecutor.utils.InitErrorJob;
import azkaban.jobExecutor.utils.JobExecutionException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class JobTypeManager 
{
	
	private final String jobtypePluginDir;	// the dir for jobtype plugins
	private final ClassLoader parentLoader; 
	
	private static final String JOBTYPECONFFILE = "plugin.properties"; // need jars.to.include property, will be loaded with user property
	private static final String JOBTYPESYSCONFFILE = "private.properties"; // not exposed to users
	private static final String COMMONCONFFILE = "common.properties";	// common properties for multiple plugins
	private static final String COMMONSYSCONFFILE = "commonprivate.properties"; // common private properties for multiple plugins
	private static final Logger logger = Logger.getLogger(JobTypeManager.class);
	
	private Map<String, Class<? extends Job>> jobToClass;
	private Map<String, Props> jobtypeJobProps;
	private Map<String, Props> jobtypeSysProps;

	public JobTypeManager(String jobtypePluginDir, ClassLoader parentClassLoader)
	{
		this.jobtypePluginDir = jobtypePluginDir;
		this.parentLoader = parentClassLoader;
		
		jobToClass = new HashMap<String, Class<? extends Job>>();
		jobtypeJobProps = new HashMap<String, Props>();
		jobtypeSysProps = new HashMap<String, Props>();
		
		loadDefaultTypes();
		
		if(jobtypePluginDir != null) {
			logger.info("job type plugin directory set. Loading extra job types.");
			loadPluginJobTypes();
		}
		
	}

	private void loadDefaultTypes() throws JobTypeManagerException{
		jobToClass.put("command", ProcessJob.class);
		jobToClass.put("javaprocess", JavaProcessJob.class);
		jobToClass.put("propertyPusher", NoopJob.class);
		jobToClass.put("python", PythonJob.class);
		jobToClass.put("ruby", RubyJob.class);
		jobToClass.put("script", ScriptJob.class);	
	}

	// load Job Typs from dir
	private void loadPluginJobTypes() throws JobTypeManagerException
	{
		if(jobtypePluginDir == null || parentLoader == null) throw new JobTypeManagerException("JobTypeDir not set! JobTypeManager not properly initiated!");
		
		File jobPluginsDir = new File(jobtypePluginDir);
		if(!jobPluginsDir.isDirectory()) throw new JobTypeManagerException("Job type plugin dir " + jobtypePluginDir + " is not a directory!");
		if(!jobPluginsDir.canRead()) throw new JobTypeManagerException("Job type plugin dir " + jobtypePluginDir + " is not readable!");
		
		// look for global conf
		Props globalConf = null;
		Props globalSysConf = null;
		File confFile = findFilefromDir(jobPluginsDir, COMMONCONFFILE);
		File sysConfFile = findFilefromDir(jobPluginsDir, COMMONSYSCONFFILE);
		try {
			if(confFile != null) {
				globalConf = new Props(null, confFile);
			}
			if(sysConfFile != null) {
				globalSysConf = new Props(null, sysConfFile);
			}
		}
		catch (Exception e) {
			throw new JobTypeManagerException("Failed to get global jobtype properties" + e.getCause());
		}
		
		for(File dir : jobPluginsDir.listFiles()) {
			if(dir.isDirectory() && dir.canRead()) {
				// get its conf file
				try {
					loadJob(dir, globalConf, globalSysConf);
				}
				catch (Exception e) {
					throw new JobTypeManagerException(e);
				}
			}
		}

	}
	
	public static File findFilefromDir(File dir, String fn){
		if(dir.isDirectory()) {
			for(File f : dir.listFiles()) {
				if(f.getName().equals(fn)) {
					return f;
				}
			}
		}
		return null;
	}
	
//	private void loadJobType(File dir, Props globalConf, Props globalSysConf) throws JobTypeManagerException{
//		
//		// look for common conf
//		Props conf = null;
//		Props sysConf = null;
//		File confFile = findFilefromDir(dir, COMMONCONFFILE);
//		File sysConfFile = findFilefromDir(dir, COMMONSYSCONFFILE);
//		
//		try {
//			if(confFile != null) {
//				conf = new Props(globalConf, confFile);
//			}
//			else {
//				conf = globalConf;
//			}
//			if(sysConfFile != null) {
//				sysConf = new Props(globalSysConf, sysConfFile);
//			}
//			else {
//				sysConf = globalSysConf;
//			}
//		}
//		catch (Exception e) {
//			throw new JobTypeManagerException("Failed to get common jobtype properties" + e.getCause());
//		}
//		
//		// look for jobtypeConf.properties and load it 		
//		for(File f: dir.listFiles()) {
//			if(f.isFile() && f.getName().equals(JOBTYPESYSCONFFILE)) {
//				loadJob(dir, f, conf, sysConf);
//				return;
//			}
//		}
//
//		// no hit, keep looking
//		for(File f : dir.listFiles()) {
//			if(f.isDirectory() && f.canRead())
//				loadJobType(f, conf, sysConf);
//		}
//		
//	}
	
	@SuppressWarnings("unchecked")
	private void loadJob(File dir, Props commonConf, Props commonSysConf) throws JobTypeManagerException{
		
		
		Props conf = null;
		Props sysConf = null;
		File confFile = findFilefromDir(dir, JOBTYPECONFFILE);		
		File sysConfFile = findFilefromDir(dir, JOBTYPESYSCONFFILE);
		if(sysConfFile == null) {
			logger.info("No job type found in " + dir.getAbsolutePath());
			return;
		}
		
		try {
			if(confFile != null) {
				conf = new Props(commonConf, confFile);
				conf = PropsUtils.resolveProps(conf);
			}
			if(sysConfFile != null) {
				sysConf = new Props(commonSysConf, sysConfFile);
				sysConf = PropsUtils.resolveProps(sysConf);
			}
		}
		catch (Exception e) {
			throw new JobTypeManagerException("Failed to get jobtype properties", e);
		}
		
		// use directory name as job type name
		String jobtypeName = dir.getName();
		
		String jobtypeClass = sysConf.get("jobtype.class");
		
		logger.info("Loading jobtype " + jobtypeName );

		// sysconf says what jars/confs to load
		List<String> jobtypeClasspath = sysConf.getStringList("jobtype.classpath", null, ",");
		List<URL> resources = new ArrayList<URL>();		
		for(String s : jobtypeClasspath) {
			try {
				File path = new File(s);
				if(path.isDirectory()) {
					for(File f : path.listFiles()) {
						resources.add(f.toURI().toURL());
						logger.info("adding to classpath " + f);
					}
				}
				resources.add(path.toURI().toURL());
				logger.info("adding to classpath " + path);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				throw new JobTypeManagerException(e);
			}
		}
		
		// each job type can have a different class loader
		ClassLoader jobTypeLoader = new URLClassLoader(resources.toArray(new URL[resources.size()]), parentLoader);
		
		Class<? extends Job> clazz = null;
		try {
			clazz = (Class<? extends Job>)jobTypeLoader.loadClass(jobtypeClass);
			jobToClass.put(jobtypeName, clazz);
		}
		catch (ClassNotFoundException e) {
			throw new JobTypeManagerException(e);
		}
		logger.info("Loaded jobtype " + jobtypeName + " " + jobtypeClass);
		
		if(conf != null) jobtypeJobProps.put(jobtypeName, conf);
		jobtypeSysProps.put(jobtypeName, sysConf);
		
	}
	
	public Job buildJobExecutor(String jobId, Props jobProps, Logger logger) throws JobTypeManagerException
	{

		Job job;
		try {
			String jobType = jobProps.getString("type");
			if (jobType == null || jobType.length() == 0) {
				/*throw an exception when job name is null or empty*/
				throw new JobExecutionException (
						String.format("The 'type' parameter for job[%s] is null or empty", jobProps, logger));
			}
			
			logger.info("Building " + jobType + " job executor. ");		
			
			Class<? extends Object> executorClass = jobToClass.get(jobType);

			if (executorClass == null) {
				throw new JobExecutionException(
						String.format("Could not construct job[%s] of type[%s].", jobProps, jobType));
			}
			
			Props sysConf = jobtypeSysProps.get(jobType);
			
			Props jobConf = jobProps;
			if(jobtypeJobProps.containsKey(jobType)) {
				Props p = jobtypeJobProps.get(jobType);
				for(String k : p.getKeySet())
				{
					if(!jobProps.containsKey(k)) {
						jobProps.put(k, p.get(k));
					}
				}
			}
			else {
				jobConf = jobProps;
			}

			if (sysConf != null) {
				sysConf = PropsUtils.resolveProps(sysConf);
			}
			else {
				sysConf = new Props();
			}

			jobConf = PropsUtils.resolveProps(jobConf);
			
//			logger.info("sysConf is " + sysConf);
//			logger.info("jobConf is " + jobConf);
			
			job = (Job)Utils.callConstructor(executorClass, jobId, sysConf, jobConf, logger);
			logger.info("job built.");
		}
		catch (Exception e) {
			job = new InitErrorJob(jobId, e);
			//throw new JobTypeManagerException(e);
		}

		return job;
	}

	public void registerJobType(String typeName, Class<? extends Job> jobTypeClass) {
		jobToClass.put(typeName, jobTypeClass);
	}
}

