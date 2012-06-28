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
	
	public static void loadProjectFlows(File dir, Map<String, Flow> output, List<String> projectErrors) {
		// Load all the project and job files.
		Map<String,Node> jobMap = new HashMap<String,Node>();
		List<Props> propsList = new ArrayList<Props>();
		Set<String> duplicateJobs = new HashSet<String>();
		Set<String> errors = new HashSet<String>();
		loadProjectFromDir(dir.getPath(), dir, jobMap, propsList, duplicateJobs, errors);
		
		// Create edge dependency sets.
		Map<String, Set<Edge>> dependencies = new HashMap<String, Set<Edge>>();
		resolveDependencies(jobMap, duplicateJobs, dependencies, errors);

		// We add all the props for the flow. Each flow will be able to keep an independent list of dependencies.
		HashMap<String, Flow> flows = buildFlowsFromDependencies(jobMap, dependencies, errors);
		for (Flow flow: flows.values()) {
			flow.addAllProperties(propsList);
		}
		
		output.putAll(flows);
		projectErrors.addAll(errors);
	}
	
	/**
	 * Loads all the files, prop and job files. Props are assigned to the job nodes.
	 */
	private static void loadProjectFromDir(String base, File dir, Map<String, Node> jobMap, List<Props> propsList, Set<String> duplicateJobs, Set<String> errors) {

		// Load all property files
		File[] propertyFiles = dir.listFiles(new SuffixFilter(PROPERTY_SUFFIX));
		Props parent = null;
		for (File file: propertyFiles) {
			String relative = getRelativeFilePath(base, file.getPath());
			try {
				parent = new Props(parent, file);
				parent.setSource(relative);
				
			} catch (IOException e) {
				errors.add("Error loading properties " + file.getName() + ":" + e.getMessage());
			}
			
			System.out.println("Adding " + relative);
			propsList.add(parent);
		}
		
		// Load all Job files. If there's a duplicate name, then we don't load
		File[] jobFiles = dir.listFiles(new SuffixFilter(JOB_SUFFIX));
		for (File file: jobFiles) {
			String jobName = getNameWithoutExtension(file);
			try {
				if (!duplicateJobs.contains(jobName)) {
					if (jobMap.containsKey(jobName)) {
						errors.add("Duplicate job names found '" + jobName + "'.");
						duplicateJobs.add(jobName);
						jobMap.remove(jobName);
					}
					else {
						Props prop = new Props(parent, file);
						String relative = getRelativeFilePath(base, file.getPath());
						prop.setSource(relative);
						
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
			loadProjectFromDir(base, file, jobMap, propsList, duplicateJobs, errors);
		}
		
	}
	
	private static void resolveDependencies(Map<String, Node> jobMap, Set<String> duplicateJobs, Map<String, Set<Edge>> nodeDependencies, Set<String> errors) {
		// Add all the in edges and out edges. Catch bad dependencies and self referrals. Also collect list of nodes who are parents.
		for (Node node: jobMap.values()) {
			List<String> dependencyList = node.getProps().getStringList(DEPENDENCIES, (List<String>)null);
			
			if (dependencyList != null) {
				Set<Edge> dependencies = nodeDependencies.get(node.getId());
				if (dependencies == null) {
					dependencies = new HashSet<Edge>();
					
					for (String dependencyName : dependencyList) {
						if (dependencyName == null || dependencyName.trim().isEmpty()) {
							continue;
						}
						
						dependencyName = dependencyName.trim();
						Node dependencyNode = jobMap.get(dependencyName);
						if (dependencyNode == null) {
							if (duplicateJobs.contains(dependencyName)) {
								dependencies.add(new ErrorEdge(dependencyName, node, "Ambiguous Dependency. Duplicates found."));
								errors.add(node.getId() + " has ambiguous dependency " + dependencyName);
							}
							else {
								dependencies.add(new ErrorEdge(dependencyName, node, "Dependency not found."));
								errors.add(node.getId() + " cannot find dependency " + dependencyName);
							}
						}
						else if (dependencyNode == node) {
							// We have a self cycle
							dependencies.add(new ErrorEdge(dependencyName, node, "Self cycle found."));
							errors.add(node.getId() + " has a self cycle");
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
	
	private static HashMap<String, Flow> buildFlowsFromDependencies(Map<String, Node> nodes, Map<String, Set<Edge>> nodeDependencies, Set<String> errors) {
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
				constructFlow(flow, base, nodes, nodeDependencies, visitedNodes, errors);
				flows.put(base.getId(), flow);
			}
		}

		return flows;
	}
	
	private static void constructFlow(Flow flow, Node node, Map<String, Node> nodes, Map<String, Set<Edge>> nodeDependencies, Set<String> visited, Set<String> errors) {
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
					errors.add("Cyclical dependency found at " + edge.getId());
					flow.addEdge(edge);
				}
				else {
					// This should not be null
					flow.addEdge(edge);
					Node fromNode = edge.getSource();
					constructFlow(flow, fromNode, nodes, nodeDependencies, visited, errors);					
				}
			}
		}
		
		visited.remove(node.getId());
	}
	
	private static String getNameWithoutExtension(File file) {
		String filename = file.getName();
		int index = filename.lastIndexOf('.');
		
		return index < 0 ? filename : filename.substring(0, index);
	}
	
	private static String getRelativeFilePath(String basePath, String filePath) {
		return filePath.substring(basePath.length() + 1);
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
