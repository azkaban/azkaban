package azkaban.utils;

import azkaban.utils.FileIOUtils.LogData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogSummary {
	private String command = null;
	private List<String> classpath = new ArrayList<String>();
	private List<String> params = new ArrayList<String>();
	
	private String[] statTableHeaders = null;
	private List<String[]> statTableData = new ArrayList<String[]>();
	
	private String[] summaryTableHeaders = null;
	private List<String[]> summaryTableData = new ArrayList<String[]>();
	
	public LogSummary(LogData log) {
		if (log != null) {
			parseLogData(log.getData());
		}
	}
	
	private void parseLogData(String data) {
		// Filter out all the timestamps
		data = data.replaceAll("(?m)^.*? - ", "");
		String[] lines = data.split("\n");
		
		parseCommand(lines);
		parseJobSummary(lines);
		parseJobStats(lines);
	}

	private void parseCommand(String[] lines) {
		int commandStartIndex = -1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].startsWith("Command: ")) {
				commandStartIndex = i;
				break;
			}
		}
		
		if (commandStartIndex != -1) {
			command = lines[commandStartIndex].substring(9);
			
			// Parse classpath
			Pattern p = Pattern.compile("(?:-cp|-classpath)\\s+(\\S+)");
			Matcher m = p.matcher(command);
			if (m.find()) {
				classpath = Arrays.asList(m.group(1).split(":"));
			}
			
			// Parse Pig params
			p = Pattern.compile("-param\\s+(\\S+)");
			m = p.matcher(command);
			while (m.find()) {
				params.add(m.group(1));
			}
		}
	}
	
	private void parseJobSummary(String[] lines) {
		int jobSummaryStartIndex = -1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].startsWith("HadoopVersion")) {
				jobSummaryStartIndex = i;
				break;
			}
		}
		
		if (jobSummaryStartIndex != -1) {
			String headerLine = lines[jobSummaryStartIndex];
			summaryTableHeaders = headerLine.split("\t");
			
			int tableRowIndex = jobSummaryStartIndex + 1;
			String line;
			while (!(line = lines[tableRowIndex]).equals("")) {
				summaryTableData.add(line.split("\t"));
				tableRowIndex++;
			}
		}
	}
	
	/**
	 * Parses the Pig Job Stats table that includes the max/min mapper and reduce times.
	 * Adds links to the job details pages on the job tracker.
	 * @param lines
	 */
	private void parseJobStats(String[] lines) {
		int jobStatsStartIndex = -1;
		
		Map<String, String> jobDetailUrls = new HashMap<String, String>();

		// Regex to search for URLs to job details pages.
		Pattern p = Pattern.compile(
				"https?://" + // http(s)://
				"[-\\w\\.]+" + // domain
				"(?::\\d+)?" + // port
				"/[\\w/\\.]*" + // path
				// query string
				"\\?\\S+" + 
				"(job_\\d{12}_\\d{4,})" + // job id
				"\\S*"
		);
		
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			Matcher m = p.matcher(line);
			
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
			statTableHeaders = headerLine.split("\t");
			
			int tableRowIndex = jobStatsStartIndex + 1;
			String line;
			while (!(line = lines[tableRowIndex]).equals("")) {
				String[] stats = line.split("\t");
				if (jobDetailUrls.containsKey(stats[0])) {
					stats[0] = "<a href=\"" + jobDetailUrls.get(stats[0]) + "\">" + stats[0] + "</a>";
				}
				statTableData.add(stats);
				tableRowIndex++;
			}
		}
	}
	
	public String[] getStatTableHeaders() {
		return statTableHeaders;
	}

	public List<String[]> getStatTableData() {
		return statTableData;
	}

	public String[] getSummaryTableHeaders() {
		return summaryTableHeaders;
	}

	public List<String[]> getSummaryTableData() {
		return summaryTableData;
	}
	
	public String getCommand() {
		return command;
	}

	public List<String> getClasspath() {
		return classpath;
	}

	public List<String> getParams() {
		return params;
	}
}
