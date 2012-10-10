/*
 * Copyright 2010 LinkedIn, Inc
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

package azkaban.jobExecutor.utils;

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
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import org.apache.log4j.Logger;

public class JobWrappingFactory 
{
	private static JobWrappingFactory jobWrappingFactory = null;
	
    //private String _defaultType;
    private Map<String, Class<? extends Job>> _jobToClass;

    protected JobWrappingFactory(final Map<String, Class<? extends Job>> jobTypeToClassMap)
    {
        //this._defaultType = defaultType;
        this._jobToClass = jobTypeToClassMap;
    }
    
    public static JobWrappingFactory getJobWrappingFactory ()
    {
    	if(jobWrappingFactory == null)
    	{
    		jobWrappingFactory = new JobWrappingFactory(new ImmutableMap.Builder<String, Class<? extends Job>>()
                    .put("java", JavaJob.class)
                    .put("command", ProcessJob.class)
                    .put("javaprocess", JavaProcessJob.class)
                    .put("pig", PigProcessJob.class)
                    .put("propertyPusher", NoopJob.class)
                    .put("python", PythonJob.class)
                    .put("ruby", RubyJob.class)
                    .put("script", ScriptJob.class).build());
    	}
    	return jobWrappingFactory;
    }

    public void registerJobExecutors(final Map<String, Class<? extends Job>> newJobExecutors)
    {
    	_jobToClass = newJobExecutors;
    }
    
    public Job buildJobExecutor(String jobId, Props props, Logger logger)
    {
      
      Job job;
      try {
        String jobType = props.getString("type");
        if (jobType == null || jobType.length() == 0) {
           /*throw an exception when job name is null or empty*/
          throw new JobExecutionException (
                                           String.format("The 'type' parameter for job[%s] is null or empty", props, logger));
        }
        Class<? extends Object> executorClass = _jobToClass.get(jobType);

        if (executorClass == null) {
            throw new JobExecutionException(
                    String.format(
                            "Could not construct job[%s] of type[%s].",
                            props,
                            jobType
                    ));
        }
        
        job = (Job)Utils.callConstructor(executorClass, jobId, props, logger);

      }
      catch (Exception e) {
          job = new InitErrorJob(props.getString("jobId"), e);
      }

//        // wrap up job in logging proxy
//        if (jobDescriptor.getLoggerPattern() != null) {
//        	job = new LoggingJob(_logDir, job, job.getId(), jobDescriptor.getLoggerPattern());	
//        }
//        else {
//        	job = new LoggingJob(_logDir, job, job.getId());	
//        }
        
        return job;
    }
}