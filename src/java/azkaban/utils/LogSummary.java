package azkaban.utils;

import azkaban.utils.FileIOUtils.LogData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogSummary {
	private static final String HIVE_PARSING_START = "Parsing command: ";
	private static final String HIVE_PARSING_END = "Parse Completed";
	private static final String HIVE_NUM_MAP_REDUCE_JOBS_STRING = "Total MapReduce jobs = ";
	private static final String HIVE_MAP_REDUCE_JOB_START = "Starting Job";
	private static final String HIVE_MAP_REDUCE_JOBS_SUMMARY = "MapReduce Jobs Launched:";
	
	// Regex to search for URLs to job details pages.
	private static final Pattern jobTrackerUrl = Pattern.compile(
			"https?://" + // http(s)://
			"[-\\w\\.]+" + // domain
			"(?::\\d+)?" + // port
			"/[\\w/\\.]*" + // path
			// query string
			"\\?\\S+" + 
			"(job_\\d{12}_\\d{4,})" + // job id
			"\\S*"
	);
	
	private String jobType = null;
	private List<Pair<String,String>> commandProperties = new ArrayList<Pair<String,String>>();
	
	private String[] pigStatTableHeaders = null;
	private List<String[]> pigStatTableData = new ArrayList<String[]>();
	
	private String[] pigSummaryTableHeaders = null;
	private List<String[]> pigSummaryTableData = new ArrayList<String[]>();
	
	private List<String> hiveQueries = new ArrayList<String>();
	
	// Each element in hiveQueryJobs contains a list of the jobs for a query.
	// Each job contains a list of strings of the job summary values.
	private List<List<List<String>>> hiveQueryJobs = new ArrayList<List<List<String>>>();
	
	public LogSummary(LogData log) {
		if (log != null) {
			parseLogData(log.getData());
		}
	}
	
	private void parseLogData(String data) {
		// Filter out all the timestamps
		data = data.replaceAll("(?m)^.*? - ", "");
		String[] lines = data.split("\n");
		
		if (parseCommand(lines)) {
			jobType = parseJobType(lines);
			
			if (jobType.contains("pig")) {
				parsePigJobSummary(lines);
				parsePigJobStats(lines);
			} else if (jobType.contains("hive")) {
				parseHiveQueries(lines);
			}
		}
	}

	private String parseJobType(String[] lines) {
		Pattern p = Pattern.compile("Building (\\S+) job executor");
		
		for (String line : lines) {
			Matcher m = p.matcher(line);
			if (m.find()) {
				return m.group(1);
			}
		}
		
		return null;
	}
	
	private boolean parseCommand(String[] lines) {
		int commandStartIndex = -1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].startsWith("Command: ")) {
				commandStartIndex = i;
				break;
			}
		}
		
		if (commandStartIndex != -1) {
			String command = lines[commandStartIndex].substring(9);
			commandProperties.add(new Pair<String,String>("Command", command));
			
			// Parse classpath
			Pattern p = Pattern.compile("(?:-cp|-classpath)\\s+(\\S+)");
			Matcher m = p.matcher(command);
			StringBuilder sb = new StringBuilder();
			if (m.find()) {
				sb.append(StringUtils.join((Collection<String>)Arrays.asList(m.group(1).split(":")), "<br/>"));
				commandProperties.add(new Pair<String,String>("Classpath", sb.toString()));
			}
			
			// Parse environment variables
			p = Pattern.compile("-D(\\S+)");
			m = p.matcher(command);
			sb = new StringBuilder();
			while (m.find()) {
				sb.append(m.group(1) + "<br/>");
			}
			if (sb.length() > 0) {
				commandProperties.add(new Pair<String,String>("-D", sb.toString()));
			}
			
			// Parse memory settings
			p = Pattern.compile("(-Xm\\S+)");
			m = p.matcher(command);
			sb = new StringBuilder();
			while (m.find()) {
				sb.append(m.group(1) + "<br/>");
			}
			if (sb.length() > 0) {
				commandProperties.add(new Pair<String,String>("Memory Settings", sb.toString()));
			}
			
			// Parse Pig params
			p = Pattern.compile("-param\\s+(\\S+)");
			m = p.matcher(command);
			sb = new StringBuilder();
			while (m.find()) {
				sb.append(m.group(1) + "<br/>");
			}
			if (sb.length() > 0) {
				commandProperties.add(new Pair<String,String>("Params", sb.toString()));
			}
			
			return true;
		}
		
		return false;
	}
	
	private void parsePigJobSummary(String[] lines) {
		int jobSummaryStartIndex = -1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].startsWith("HadoopVersion")) {
				jobSummaryStartIndex = i;
				break;
			}
		}
		
		if (jobSummaryStartIndex != -1) {
			String headerLine = lines[jobSummaryStartIndex];
			pigSummaryTableHeaders = headerLine.split("\t");
			
			int tableRowIndex = jobSummaryStartIndex + 1;
			String line;
			while (!(line = lines[tableRowIndex]).equals("")) {
				pigSummaryTableData.add(line.split("\t"));
				tableRowIndex++;
			}
		}
	}
	
	/**
	 * Parses the Pig Job Stats table that includes the max/min mapper and reduce times.
	 * Adds links to the job details pages on the job tracker.
	 * @param lines
	 */
	private void parsePigJobStats(String[] lines) {
		int jobStatsStartIndex = -1;
		
		Map<String, String> jobDetailUrls = new HashMap<String, String>();

		
		
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			Matcher m = jobTrackerUrl.matcher(line);
			
			if (m.find()) {
				jobDetailUrls.put(m.group(1), m.group(0));
			}
			else if (line.startsWith("Job Stats (time in seconds):")) {
				jobStatsStartIndex = i+1;
				break;
			}
		}
		
		if (jobStatsStartIndex != -1) {
			String headerLine = lines[jobStatsStartIndex];
			pigStatTableHeaders = headerLine.split("\t");
			
			int tableRowIndex = jobStatsStartIndex + 1;
			String line;
			while (!(line = lines[tableRowIndex]).equals("")) {
				String[] stats = line.split("\t");
				if (jobDetailUrls.containsKey(stats[0])) {
					stats[0] = "<a href=\"" + jobDetailUrls.get(stats[0]) + "\">" + stats[0] + "</a>";
				}
				pigStatTableData.add(stats);
				tableRowIndex++;
			}
		}
	}
	
	private void parseHiveQueries(String[] lines) {
		for (int i = 0; i < lines.length;) {
			String line = lines[i];
			int parsingCommandIndex = line.indexOf(HIVE_PARSING_START);
			if (parsingCommandIndex != -1) {
				// parse query text
				int queryStartIndex = parsingCommandIndex + HIVE_PARSING_START.length();
				StringBuilder query = new StringBuilder(line.substring(queryStartIndex) + "\n");
				
				i++;
				while (i < lines.length && !(line = lines[i]).contains(HIVE_PARSING_END)) {
					query.append(line + "\n");
					i++;
				}
				String queryString = query.toString().trim().replaceAll("\n","<br/>");
				hiveQueries.add(queryString);
				i++;
				
				// parse the query's Map-Reduce jobs, if any.
				int numMRJobs = 0;
				List<String> jobTrackerUrls = new ArrayList<String>();
				while (i < lines.length) {
					line = lines[i];
					if (line.contains(HIVE_NUM_MAP_REDUCE_JOBS_STRING)) {
						// query involves map reduce jobs
						numMRJobs = Integer.parseInt(line.substring(HIVE_NUM_MAP_REDUCE_JOBS_STRING.length()));
						i++;
						
						// get the job tracker URLs
						String lastUrl = "";
						int numJobsSeen = 0;
						while (numJobsSeen < numMRJobs && i < lines.length) {
							line = lines[i];
							if (line.contains(HIVE_MAP_REDUCE_JOB_START)) {
								Matcher m = jobTrackerUrl.matcher(line);
								if (m.find() && !lastUrl.equals(m.group(1))) {
									jobTrackerUrls.add(m.group(0));
									lastUrl = m.group(1);
									numJobsSeen++;
								}
							}
							i++;
						}
						
						// get the map reduce jobs summary
						while (i < lines.length) {
							line = lines[i];
							if (line.contains(HIVE_MAP_REDUCE_JOBS_SUMMARY)) {
								// job summary table found
								i++;
								
								List<List<String>> queryJobs = new ArrayList<List<String>>();
								
								Pattern p = Pattern.compile(
									"Job (\\d+): Map: (\\d+)  Reduce: (\\d+)   HDFS Read: (\\d+) HDFS Write: (\\d+)"
								);
								
								int previousJob = -1;
								numJobsSeen = 0;
								while (numJobsSeen < numMRJobs && i < lines.length) {
									line = lines[i];
									Matcher m = p.matcher(line);
									if (m.find()) {
										int currJob = Integer.parseInt(m.group(1));
										if (currJob == previousJob) {
											i++;
											continue;
										}
										
										List<String> job = new ArrayList<String>();
										job.add("<a href=\"" + jobTrackerUrls.get(currJob) +
												"\">" + currJob + "</a>");
										job.add(m.group(2));
										job.add(m.group(3));
										job.add(m.group(4));
										job.add(m.group(5));
										queryJobs.add(job);
										previousJob = currJob;
										numJobsSeen++;
									}
									i++;
								}
								
								if (numJobsSeen == numMRJobs) {
									hiveQueryJobs.add(queryJobs);
								}
								
								break;
							}
							i++;
						} 
						break;
					}
					else if (line.contains(HIVE_PARSING_START)) {
						if (numMRJobs == 0) {
							hiveQueryJobs.add(null);
						}
						break;
					}
					i++;
				}
				continue;
			}
			
			i++;
		}
		return;
	}
	
	public String[] getPigStatTableHeaders() {
		return pigStatTableHeaders;
	}

	public List<String[]> getPigStatTableData() {
		return pigStatTableData;
	}

	public String[] getPigSummaryTableHeaders() {
		return pigSummaryTableHeaders;
	}

	public List<String[]> getPigSummaryTableData() {
		return pigSummaryTableData;
	}
	
	public String getJobType() {
		return jobType;
	}
	
	public List<Pair<String,String>> getCommandProperties() {
		return commandProperties;
	}

	public List<String> getHiveQueries() {
		return hiveQueries;
	}

	public List<List<List<String>>> getHiveQueryJobs() {
		return hiveQueryJobs;
	}
}
