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
import azkaban.jobExecutor.JavaJob;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.NoopJob;
import azkaban.jobExecutor.PigProcessJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.PythonJob;
import azkaban.jobExecutor.RubyJob;
import azkaban.jobExecutor.ScriptJob;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.jobExecutor.utils.JobExecutionException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class JobtypeManager 
{
	
	private final String jobtypePluginDir;	// the dir for jobtype plugins
	private final ClassLoader parentLoader; 
	
	private static final String jobtypeConfFile = "jobtype.conf"; // need jars.to.include property
	private static final Logger logger = Logger.getLogger(JobtypeManager.class);
	
	private Map<String, Class<? extends Job>> jobToClass;
	private Map<String, Props> jobTypeProps;

	public JobtypeManager(String jobtypePluginDir, ClassLoader parentClassLoader)
	{
		this.jobtypePluginDir = jobtypePluginDir;
		this.parentLoader = parentClassLoader;
		
		jobToClass = new HashMap<String, Class<? extends Job>>();
		jobTypeProps = new HashMap<String, Props>();
		
		loadDefaultTypes(jobToClass, jobTypeProps);
		
		if(jobtypePluginDir != null) {
			logger.info("job type plugin directory set. Loading extra job types.");
			loadPluginJobTypes(jobToClass, jobTypeProps);;
		}
		
	}

	private void loadDefaultTypes(Map<String, Class<? extends Job>> allJobTypes, Map<String, Props> jobTypeProps) throws JobtypeManagerException{
		allJobTypes.put("java", JavaJob.class);
		allJobTypes.put("command", ProcessJob.class);
		allJobTypes.put("javaprocess", JavaProcessJob.class);
		allJobTypes.put("pig", PigProcessJob.class);
		allJobTypes.put("propertyPusher", NoopJob.class);
		allJobTypes.put("python", PythonJob.class);
		allJobTypes.put("ruby", RubyJob.class);
		allJobTypes.put("script", ScriptJob.class);		
		
		jobTypeProps.put("java", new Props());
		jobTypeProps.put("command", new Props());
		jobTypeProps.put("javaprocess", new Props());
		jobTypeProps.put("pig", new Props());
		jobTypeProps.put("propertyPusher", new Props());
		jobTypeProps.put("python", new Props());
		jobTypeProps.put("ruby", new Props());
		jobTypeProps.put("script", new Props());	
	}

	// load Job Typs from dir
	private void loadPluginJobTypes(Map<String, Class<? extends Job>> allJobTypes, Map<String, Props> jobTypeProps) throws JobtypeManagerException
	{
		if(jobtypePluginDir == null || parentLoader == null) throw new JobtypeManagerException("JobTypeDir not set! JobWrappingFactory not properly initiated!");
		
		File jobPluginsDir = new File(jobtypePluginDir);
		if(!jobPluginsDir.isDirectory()) throw new JobtypeManagerException("Job type plugin dir " + jobtypePluginDir + " is not a directory!");
		if(!jobPluginsDir.canRead()) throw new JobtypeManagerException("Job type plugin dir " + jobtypePluginDir + " is not readable!");
		
		
		
		for(File dir : jobPluginsDir.listFiles()) {
			if(dir.isDirectory() && dir.canRead()) {
				// get its conf file
				try {
					loadJobType(dir, allJobTypes, jobTypeProps);
				}
				catch (Exception e) {
					throw new JobtypeManagerException(e);
				}
			}
		}

	}
	
	public void loadJobType(File dir, Map<String, Class<? extends Job>> allJobTypes, Map<String, Props> jobTypeProps) throws JobtypeManagerException{
		
		// look for jobtypeConf.properties and load it 		
		for(File f: dir.listFiles()) {
			if(f.isFile() && f.getName().equals(jobtypeConfFile)) {
				loadJob(dir, f, allJobTypes, jobTypeProps);
				return;
			}
		}

		// no hit, keep looking
		for(File f : dir.listFiles()) {
			if(f.isDirectory() && f.canRead())
				loadJobType(f, allJobTypes, jobTypeProps);
			
			
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public void loadJob(File dir, File jobConfFile, Map<String, Class<? extends Job>> allJobTypes, Map<String, Props> jobTypeProps) throws JobtypeManagerException{
		Props props;
		try {
			props = new Props(null, jobConfFile);
		} catch (IOException e) {
			throw new JobtypeManagerException(e);
		}
		
		List<String> jobtypeClasspath = props.getStringList("jobtype.classpath", null, ",");

		// use directory name as job type name
		String jobtypeName = dir.getName();
		String jobtypeClass = props.get("jobtype.class");
		
		logger.info("Loading jobtype " + jobtypeName );
//		List<URL> resources = new ArrayList<URL>();
//		for(File f : dir.listFiles()) {
//			try {
//				resources.add(f.toURI().toURL());
//			} catch (MalformedURLException e) {
//				// TODO Auto-generated catch block
//				throw new JobWrappingFactoryException(e);
//			}
//		}
		List<URL> resources = new ArrayList<URL>();
		for(String path : jobtypeClasspath) {
			URL res;
			try {
				res = new File(path).toURI().toURL();
				logger.info("adding to classpath " + res);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				throw new JobtypeManagerException(e);
			}
			resources.add(res);
		}
		
		// each job type can have a different class loader
		ClassLoader jobTypeLoader = new URLClassLoader(resources.toArray(new URL[resources.size()]), parentLoader);
		
		Class<? extends Job> clazz = null;
		try {
			clazz = (Class<? extends Job>)jobTypeLoader.loadClass(jobtypeClass);
			allJobTypes.put(jobtypeName, clazz);
		}
		catch (ClassNotFoundException e) {
			throw new JobtypeManagerException(e);
		}
		logger.info("Loaded jobtype " + jobtypeName + " " + jobtypeClass);
		jobTypeProps.put(jobtypeName, props);
	}
	
	public Job buildJobExecutor(String jobId, Props props, Logger logger) throws JobtypeManagerException
	{

		Job job;
		try {
			String jobType = props.getString("type");
			if (jobType == null || jobType.length() == 0) {
				/*throw an exception when job name is null or empty*/
				throw new JobExecutionException (
						String.format("The 'type' parameter for job[%s] is null or empty", props, logger));
			}
			
			logger.info("Building " + jobType + " job executor. ");			
			Class<? extends Object> executorClass = jobToClass.get(jobType);

			if (executorClass == null) {
				throw new JobExecutionException(
						String.format("Could not construct job[%s] of type[%s].", props, jobType));
			}
			
			job = (Job)Utils.callConstructor(executorClass, jobId, props, logger);

		}
		catch (Exception e) {
			//job = new InitErrorJob(jobId, e);
			throw new JobtypeManagerException(e);
		}

		return job;
	}


}

