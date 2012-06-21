package azkaban.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.flow.Edge;
import azkaban.flow.ErrorEdge;
import azkaban.flow.Flow;
import azkaban.flow.Node;

public class FlowUtils {
	private static final DirFilter DIR_FILTER = new DirFilter();
	private static final String PROPERTY_SUFFIX = ".properties";
	private static final String DEPENDENCIES = "dependencies";
	private static final String JOB_SUFFIX = ".job";
	
	public static Map<String, Flow> loadProject(File dir) {
		String base = dir.getAbsolutePath();
		
		// Load all the project and job files.
		Map<String,Node> jobMap = new HashMap<String,Node>();
		Set<String> duplicateJobs = new HashSet<String>();
		List<String> errors = new ArrayList<String>();
		loadProjectFromDir(base, dir, jobMap, duplicateJobs, errors);
		
		// Create edge dependency sets.
		Map<String, Set<Edge>> dependencies = new HashMap<String, Set<Edge>>();
		resolveDependencies(jobMap, duplicateJobs, dependencies);

		HashMap<String, Flow> flows = buildFlowsFromDependencies(jobMap, dependencies);
		return flows;
	}
	
	/**
	 * Loads all the files, prop and job files. Props are assigned to the job nodes.
	 */
	private static void loadProjectFromDir(String baseDir, File dir, Map<String, Node> jobMap, Set<String> duplicateJobs, List<String> errors) {
		// Load all property files
		File[] propertyFiles = dir.listFiles(new SuffixFilter(PROPERTY_SUFFIX));
		Props parent = null;
		for (File file: propertyFiles) {
			try {
				parent = new Props(parent, file);
			} catch (IOException e) {
				errors.add("Error loading properties " + file.getName() + ":" + e.getMessage());
			}
		}
		
		// Load all Job files. If there's a duplicate name, then we don't load
		File[] jobFiles = dir.listFiles(new SuffixFilter(JOB_SUFFIX));
		for (File file: jobFiles) {
			try {
				String jobName = getJobName(file, JOB_SUFFIX);

				if (!duplicateJobs.contains(jobName)) {
					if (jobMap.containsKey(jobName)) {
						duplicateJobs.add(jobName);
						jobMap.remove(jobName);
					}
					else {
						Props prop = new Props(parent, file);
						Node node = new Node(jobName, prop);
					
						jobMap.put(jobName, node);
					}
				}
				
			} catch (IOException e) {
				errors.add("Error loading job file " + file.getName() + ":" + e.getMessage());
			}
		}
		
		File[] subDirs = dir.listFiles(DIR_FILTER);
		for (File file: subDirs) {
			loadProjectFromDir(baseDir, file, jobMap, duplicateJobs, errors);
		}
		
	}
	
	private static void resolveDependencies(Map<String, Node> jobMap, Set<String> duplicateJobs, Map<String, Set<Edge>> nodeDependencies) {
		// Add all the in edges and out edges. Catch bad dependencies and self referrals. Also collect list of nodes who are parents.
		for (Node node: jobMap.values()) {
			List<String> dependencyList = node.getProps().getStringList(DEPENDENCIES, (List<String>)null);
			
			if (duplicateJobs != null) {
				Set<Edge> dependencies = nodeDependencies.get(node.getId());
				if (dependencies == null) {
					dependencies = new HashSet<Edge>();
					
					for (String dependencyName : dependencyList) {
						if (dependencyName == null || dependencyName.trim().isEmpty()) {
							continue;
						}
						
						Node dependencyNode = jobMap.get(dependencyName);
						if (dependencyNode == null) {
							if (duplicateJobs.contains(dependencyName)) {
								dependencies.add(new ErrorEdge(dependencyName, node, "Ambiguous Dependency. Duplicates found."));
							}
							else {
								dependencies.add(new ErrorEdge(dependencyName, node, "Dependency not found."));
							}
						}
						else if (dependencyNode == node) {
							// We have a self cycle
							dependencies.add(new ErrorEdge(dependencyName, node, "Self cycle found."));
						}
						else {
							dependencies.add(new Edge(dependencyNode, node));
						}
					}
					
					if (!dependencies.isEmpty()) {
						nodeDependencies.put(node.getId(), dependencies);
					}
				}
			}
		}
		
	}
	
	private static HashMap<String, Flow> buildFlowsFromDependencies(Map<String, Node> nodes, Map<String, Set<Edge>> nodeDependencies) {
		// Find all root nodes by finding ones without dependents.
		HashSet<String> nonRootNodes = new HashSet<String>();
		for (Set<Edge> edges: nodeDependencies.values()) {
			for (Edge edge: edges) {
				nonRootNodes.add(edge.getSourceId());
			}
		}
		
		
		HashMap<String, Flow> flows = new HashMap<String, Flow>();
		
		// Now create flows. Bad flows are marked invalid
		Set<String> visitedNodes = new HashSet<String>();
		for (Node base: nodes.values()) {
			if (!nonRootNodes.contains(base)) {
				Flow flow = new Flow(base.getId());
				flow.addBaseNode(base);
				constructFlow(flow, base, nodes, nodeDependencies, visitedNodes);
				flows.put(base.getId(), flow);
			}
		}

		return flows;
	}
	
	private static void constructFlow(Flow flow, Node node, Map<String, Node> nodes, Map<String, Set<Edge>> nodeDependencies, Set<String> visited) {
		visited.add(node.getId());
		flow.addNode(node);
		Set<Edge> dependencies = nodeDependencies.get(node.getId());
		
		if (dependencies != null) {
			for (Edge edge: dependencies) {
				if (edge instanceof ErrorEdge) {
					flow.addEdge(edge);
				}
				else if (visited.contains(edge.getSourceId())){
					// We have a cycle. We set it as an error edge
					edge = new ErrorEdge(edge.getSourceId(), node, "Cyclical dependencies found.");
					flow.addEdge(edge);
				}
				else {
					// This should not be null
					flow.addEdge(edge);
					Node fromNode = edge.getSource();
					constructFlow(flow, fromNode, nodes, nodeDependencies, visited);					
				}
			}
		}
		
		visited.remove(node.getId());
	}
	
	private static String getJobName(File file, String suffix) {
		String filename = file.getName();
		return filename.substring(0, filename.length() - JOB_SUFFIX.length());
	}

	private static class DirFilter implements FileFilter {
		@Override
		public boolean accept(File pathname) {
			return pathname.isDirectory();
		}
	}
	
	private static class SuffixFilter implements FileFilter {
		private String suffix;
		
		public SuffixFilter(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();
			
			return pathname.isFile() && !pathname.isHidden() && name.length() > suffix.length() && name.endsWith(suffix);
		}
	}
}
