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
package azkaban.jobExecutor;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import azkaban.utils.Props;

public class PythonJob extends LongArgJob {

	private static final String PYTHON_BINARY_KEY = "python";
	private static final String SCRIPT_KEY = "script";

	public PythonJob(String jobid, Props props, Logger log) {
		super(jobid, 
				new String[] { props.getString(PYTHON_BINARY_KEY, "python"),props.getString(SCRIPT_KEY) }, 
				props, 
				log, 
				ImmutableSet.of(PYTHON_BINARY_KEY, SCRIPT_KEY, JOB_TYPE));
	}

}
