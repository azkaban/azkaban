package azkaban.utils;

import azkaban.utils.FileIOUtils.LogData;

import java.util.ArrayList;
import java.util.List;

public class LogSummary {
	private String[] statTableHeaders = null;
	private List<String[]> statTableData = new ArrayList<String[]>();
	
	public LogSummary(LogData log) {
		if (log != null) {
			parseLogData(log.getData());
		}
	}
	
	public String[] getStatTableHeaders() {
		return statTableHeaders;
	}

	public List<String[]> getStatTableData() {
		return statTableData;
	}

	private void parseLogData(String data) {
		data = data.replaceAll(".*? - ", "");
		String[] lines = data.split("\n");
		
		int jobStatsStartIndex = -1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].startsWith("Job Stats (time in seconds):")) {
				jobStatsStartIndex = i+1;
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
}
