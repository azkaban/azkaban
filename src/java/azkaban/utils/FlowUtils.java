package azkaban.utils;

import java.io.File;
import java.io.FileNamFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;

public class FlowUtils {
	public static void loadProjectFilesFrom(File dir, List<Flow> resultFlow, List<Props> resultProps) {
		File[] propertyFiles = dir.listFiles();
		ArrayList<String> errors = new ArrayList<String>();
		Props propertyFile = null;
		for (File file: propertyFiles) {
			
		}
	}
	
	private static void loadProjectFromDir(File dir, Map<String, Node> node, Map<String, Props> propsFiles, List<String> errors) {
		File[] propertyFiles = dir.listFiles(new SuffixFilter(".properties"));
		Props parent = null;
		for (File file: propertyFiles) {
			try {
				Props props = new Props(parent, file);
			} catch (IOException e) {
				errors.add("Error loading properties " + file.getName() + ":" + e.getMessage());
			}
		}
	}
	
	private class SuffixFilter implements FilenameFilter {
		private String suffix;
		
		public SuffixFilter(String suffix) {
			this.suffix = suffix;
		}
		
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(suffix);
		}
		
	}
}
