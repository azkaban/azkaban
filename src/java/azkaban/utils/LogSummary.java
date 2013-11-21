package azkaban.utils;

import azkaban.utils.FileIOUtils.LogData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
		data = data.replaceAll(".*? - ", "");
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
	
	private void parseJobStats(String[] lines) {
		int jobStatsStartIndex = -1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].startsWith("Job Stats (time in seconds):")) {
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
				statTableData.add(line.split("\t"));
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
