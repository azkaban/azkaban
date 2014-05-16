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

package azkaban.flow;

public class CommonJobProperties {
	/*
	 * The following are Common properties that can be set in a job file
	 */
	
	/**
	 * The type of job that will be executed.
	 * Examples: command, java, etc.
	 */
	public static final String JOB_TYPE = "type";
	
	/**
	 * Force a node to be a root node in a flow, even if there are other jobs dependent on it.
	 */
	public static final String ROOT_NODE = "root.node";
	
	/**
	 * Comma delimited list of job names which are dependencies
	 */
	public static final String DEPENDENCIES = "dependencies";
	
	/**
	 * The number of retries when this job has failed.
	 */
	public static final String RETRIES = "retries";
	
	/**
	 * The time in millisec to back off after every retry
	 */
	public static final String RETRY_BACKOFF = "retry.backoff";
	
	/**
	 * Comma delimited list of email addresses for both failure and success messages
	 */
	public static final String NOTIFY_EMAILS = "notify.emails";
	
	/**
	 * Comma delimited list of email addresses for success messages
	 */
	public static final String SUCCESS_EMAILS = "success.emails";
	
	/**
	 * Comma delimited list of email addresses for failure messages
	 */
	public static final String FAILURE_EMAILS = "failure.emails";

	/*
	 * The following are the common props that will be added to the job by azkaban
	 */
	
	/**
	 * The attempt number of the executing job.
	 */
	public static final String JOB_ATTEMPT = "azkaban.job.attempt";
	
	/**
	 * The attempt number of the executing job.
	 */
	public static final String JOB_METADATA_FILE = "azkaban.job.metadata.file";

	/**
	 * The attempt number of the executing job.
	 */
	public static final String JOB_ATTACHMENT_FILE = "azkaban.job.attachment.file";
	
	/**
	 * The executing flow id
	 */
	public static final String FLOW_ID = "azkaban.flow.flowid";
	
	/**
	 * The nested flow id path
	 */
	public static final String NESTED_FLOW_PATH = "azkaban.flow.nested.path";
	
	/**
	 * The execution id. This should be unique per flow, but may not be due to 
	 * restarts.
	 */
	public static final String EXEC_ID = "azkaban.flow.execid";
	
	/**
	 * The numerical project id identifier.
	 */
	public static final String PROJECT_ID = "azkaban.flow.projectid";
	
	/**
	 * The version of the project the flow is running. This may change if a
	 * forced hotspot occurs.
	 */
	public static final String PROJECT_VERSION = "azkaban.flow.projectversion";
	
	/**
	 * A uuid assigned to every execution
	 */
	public static final String FLOW_UUID = "azkaban.flow.uuid";
	
	/**
	 * Properties for passing the flow start time to the jobs.
	 */
	public static final String FLOW_START_TIMESTAMP = "azkaban.flow.start.timestamp";
	public static final String FLOW_START_YEAR = "azkaban.flow.start.year";
	public static final String FLOW_START_MONTH = "azkaban.flow.start.month";
	public static final String FLOW_START_DAY = "azkaban.flow.start.day";
	public static final String FLOW_START_HOUR = "azkaban.flow.start.hour";
	public static final String FLOW_START_MINUTE = "azkaban.flow.start.minute";
	public static final String FLOW_START_SECOND = "azkaban.flow.start.second";
	public static final String FLOW_START_MILLISSECOND = "azkaban.flow.start.milliseconds";
	public static final String FLOW_START_TIMEZONE = "azkaban.flow.start.timezone";

    public static final String FLOW_BACK_ONE_HOUR_TIMESTAMP = "azkaban.flow.one.hour.back.timestamp";
    public static final String FLOW_BACK_ONE_HOUR_YEAR = "azkaban.flow.one.hour.back.year";
    public static final String FLOW_BACK_ONE_HOUR_MONTH = "azkaban.flow.one.hour.back.month";
    public static final String FLOW_BACK_ONE_HOUR_DAY = "azkaban.flow.one.hour.back.day";
    public static final String FLOW_BACK_ONE_HOUR_HOUR = "azkaban.flow.one.hour.back.hour";
    public static final String FLOW_BACK_ONE_HOUR_MINUTE = "azkaban.flow.one.hour.back.minute";
    public static final String FLOW_BACK_ONE_HOUR_SECOND = "azkaban.flow.one.hour.back.second";
    public static final String FLOW_BACK_ONE_HOUR_MILLISSECOND = "azkaban.flow.one.hour.back.milliseconds";
    public static final String FLOW_BACK_ONE_HOUR_TIMEZONE = "azkaban.flow.one.hour.back.timezone";

    public static final String FLOW_BACK_ONE_DAY_TIMESTAMP = "azkaban.flow.one.day.back.timestamp";
    public static final String FLOW_BACK_ONE_DAY_YEAR = "azkaban.flow.one.day.back.year";
    public static final String FLOW_BACK_ONE_DAY_MONTH = "azkaban.flow.one.day.back.month";
    public static final String FLOW_BACK_ONE_DAY_DAY = "azkaban.flow.one.day.back.day";
    public static final String FLOW_BACK_ONE_DAY_HOUR = "azkaban.flow.one.day.back.hour";
    public static final String FLOW_BACK_ONE_DAY_MINUTE = "azkaban.flow.one.day.back.minute";
    public static final String FLOW_BACK_ONE_DAY_SECOND = "azkaban.flow.one.day.back.second";
    public static final String FLOW_BACK_ONE_DAY_MILLISSECOND = "azkaban.flow.one.day.back.milliseconds";
    public static final String FLOW_BACK_ONE_DAY_TIMEZONE = "azkaban.flow.one.day.back.timezone";

    public static final String FLOW_BACK_ONE_WEEK_TIMESTAMP = "azkaban.flow.one.week.back.timestamp";
    public static final String FLOW_BACK_ONE_WEEK_YEAR = "azkaban.flow.one.week.back.year";
    public static final String FLOW_BACK_ONE_WEEK_MONTH = "azkaban.flow.one.week.back.month";
    public static final String FLOW_BACK_ONE_WEEK_DAY = "azkaban.flow.one.week.back.day";
    public static final String FLOW_BACK_ONE_WEEK_HOUR = "azkaban.flow.one.week.back.hour";
    public static final String FLOW_BACK_ONE_WEEK_MINUTE = "azkaban.flow.one.week.back.minute";
    public static final String FLOW_BACK_ONE_WEEK_SECOND = "azkaban.flow.one.week.back.second";
    public static final String FLOW_BACK_ONE_WEEK_MILLISSECOND = "azkaban.flow.one.week.back.milliseconds";
    public static final String FLOW_BACK_ONE_WEEK_TIMEZONE = "azkaban.flow.one.week.back.timezone";

    public static final String FLOW_BACK_ONE_MONTH_TIMESTAMP = "azkaban.flow.one.month.back.timestamp";
    public static final String FLOW_BACK_ONE_MONTH_YEAR = "azkaban.flow.one.month.back.year";
    public static final String FLOW_BACK_ONE_MONTH_MONTH = "azkaban.flow.one.month.back.month";
    public static final String FLOW_BACK_ONE_MONTH_DAY = "azkaban.flow.one.month.back.day";
    public static final String FLOW_BACK_ONE_MONTH_HOUR = "azkaban.flow.one.month.back.hour";
    public static final String FLOW_BACK_ONE_MONTH_MINUTE = "azkaban.flow.one.month.back.minute";
    public static final String FLOW_BACK_ONE_MONTH_SECOND = "azkaban.flow.one.month.back.second";
    public static final String FLOW_BACK_ONE_MONTH_MILLISSECOND = "azkaban.flow.one.month.back.milliseconds";
    public static final String FLOW_BACK_ONE_MONTH_TIMEZONE = "azkaban.flow.one.month.back.timezone";

    public static final String FLOW_BACK_ONE_YEAR_TIMESTAMP = "azkaban.flow.one.year.back.timestamp";
    public static final String FLOW_BACK_ONE_YEAR_YEAR = "azkaban.flow.one.year.back.year";
    public static final String FLOW_BACK_ONE_YEAR_MONTH = "azkaban.flow.one.year.back.month";
    public static final String FLOW_BACK_ONE_YEAR_DAY = "azkaban.flow.one.year.back.day";
    public static final String FLOW_BACK_ONE_YEAR_HOUR = "azkaban.flow.one.year.back.hour";
    public static final String FLOW_BACK_ONE_YEAR_MINUTE = "azkaban.flow.one.year.back.minute";
    public static final String FLOW_BACK_ONE_YEAR_SECOND = "azkaban.flow.one.year.back.second";
    public static final String FLOW_BACK_ONE_YEAR_MILLISSECOND = "azkaban.flow.one.year.back.milliseconds";
    public static final String FLOW_BACK_ONE_YEAR_TIMEZONE = "azkaban.flow.one.year.back.timezone";
}
