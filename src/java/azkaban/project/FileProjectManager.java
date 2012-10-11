package azkaban.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.flow.Node;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.DirectoryFlowLoader;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

/**
 * A project loader that stores everything on local file system. The following
 * global parameters should be set - file.project.loader.path - The project
 * install path where projects will be loaded installed to.
 */
public class FileProjectManager implements ProjectManager {
	// Layout for project logging
	private static final Layout DEFAULT_LAYOUT = new PatternLayout("%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
	
	public static final String DIRECTORY_PARAM = "file.project.loader.path";
	private static final String DELETED_PROJECT_PREFIX = ".DELETED.";
	private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd-HH:mm.ss.SSS");
	private static final String PROPERTIES_FILENAME = "project.json";
	private static final String PROJECT_DIRECTORY = "src";
	private static final String FLOW_EXTENSION = ".flow";
	private static final Logger logger = Logger.getLogger(FileProjectManager.class);
	private static final int IDLE_SECONDS = 120;
	private static final long PROJECT_LOG_SIZE = 1024*512; // 512kb Log size rollover
	private static final int LOG_BACKUP = 1000;   // I think 512mb is good enough per Project.
	
	private ConcurrentHashMap<String, Project> projects = new ConcurrentHashMap<String, Project>();
	private CacheManager manager = CacheManager.create();
	private Cache sourceCache;

	private File projectDirectory;

	public FileProjectManager(Props props) {
		setupDirectories(props);
		loadAllProjects();
		setupCache();
	}

	private void setupCache() {
		CacheConfiguration cacheConfig = new CacheConfiguration("propsCache",2000)
				.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
				.overflowToDisk(false)
				.eternal(false)
				.timeToIdleSeconds(IDLE_SECONDS)
				.diskPersistent(false)
				.diskExpiryThreadIntervalSeconds(0);

		sourceCache = new Cache(cacheConfig);
		manager.addCache(sourceCache);
	}

	private void setupDirectories(Props props) {
		String projectDir = props.getString(DIRECTORY_PARAM);
		logger.info("Using directory " + projectDir + " as the project directory.");
		projectDirectory = new File(projectDir);
		
		if (!projectDirectory.exists()) {
			logger.info("Directory " + projectDir + " doesn't exist. Creating.");
			if (projectDirectory.mkdirs()) {
				logger.info("Directory creation was successful.");
			} 
			else {
				throw new RuntimeException("FileProjectLoader cannot create directory " + projectDirectory);
			}
		} 
		else if (projectDirectory.isFile()) {
			throw new RuntimeException("FileProjectManager directory " + projectDirectory + " is really a file.");
		}
	}

	private void loadAllProjects() {
		File[] directories = projectDirectory.listFiles();

		for (File dir : directories) {
			if (!dir.isDirectory()) {
				logger.error("ERROR loading project from " + dir.getPath() + ". Not a directory.");
			} 
			else if (dir.getName().startsWith(DELETED_PROJECT_PREFIX)) {
				continue;
			}
			else {
				File propertiesFile = new File(dir, PROPERTIES_FILENAME);
				if (!propertiesFile.exists()) {
					logger.error("ERROR loading project from " + dir.getPath()
							+ ". Project file " + PROPERTIES_FILENAME
							+ " not found.");
				} 
				else {
					Object obj = null;
					try {
						obj = JSONUtils.parseJSONFromFile(propertiesFile);
					} 
					catch (IOException e) {
						logger.error( "ERROR loading project from " + dir.getPath() 
								+ ". Project file " + PROPERTIES_FILENAME + " couldn't be read.", e);
						continue;
					}

					Project project = Project.projectFromObject(obj);
					logger.info("Loading project " + project.getName());
					projects.put(project.getName(), project);
					attachLoggerToProject(project);
					
					String source = project.getSource();
					if (source == null) {
						logger.info(project.getName() + ": No flows uploaded");
						continue;
					}

					File projectDir = new File(dir, source);
					if (!projectDir.exists()) {
						logger.error("ERROR project source dir " + projectDir + " doesn't exist.");
					} 
					else if (!projectDir.isDirectory()) {
						logger.error("ERROR project source dir " + projectDir + " is not a directory.");
					} 
					else {
						File[] flowFiles = projectDir.listFiles(new SuffixFilter(FLOW_EXTENSION));
						
						Map<String, Flow> flowMap = new LinkedHashMap<String, Flow>();
						for (File flowFile : flowFiles) {
							Object objectizedFlow = null;
							try {
								objectizedFlow = JSONUtils.parseJSONFromFile(flowFile);
							} 
							catch (IOException e) {
								logger.error("Error parsing flow file " + flowFile.toString(), e);
								continue;
							}

							// Recreate Flow
							Flow flow = null;

							try {
								flow = Flow.flowFromObject(objectizedFlow);
								flow.setProjectId(project.getName());
							} 
							catch (Exception e) {
								logger.error(
										"Error loading flow "
												+ flowFile.getName()
												+ " in project "
												+ project.getName(), e);
								continue;
							}
							logger.debug("Loaded flow " + project.getName() + ": " + flow.getId());
							flow.initialize();

							flowMap.put(flow.getId(), flow);
						}

						synchronized (project) {
							project.setFlows(flowMap);
						}
					}
				}
			}
		}
	}

	public List<String> getProjectNames() {
		return new ArrayList<String>(projects.keySet());
	}

	public List<Project> getUserProjects(User user) {
		ArrayList<Project> array = new ArrayList<Project>();
		for (Project project : projects.values()) {
			Permission perm = project.getUserPermission(user);

			if (perm != null && (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(Type.READ))) {
				array.add(project);
			}
		}
		return array;
	}
	
	@Override
	public List<Project> getUserProjectsByRe(User user, final String rePattern) {
		ArrayList<Project> array = new ArrayList<Project>();
		Pattern pattern;
		try {
			pattern = Pattern.compile(rePattern, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			logger.error("Bad regex pattern " + rePattern);
			return array;
		}
		
		
		for (Project project : projects.values()) {
			Permission perm = project.getUserPermission(user);

			if (perm != null && (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(Type.READ))) {
				if(pattern.matcher(project.getName()).find() ) {
					array.add(project);
				}
			}
		}
		return array;
	}

	@Override
	public List<Project> getProjects() {
		return new ArrayList<Project>(projects.values());
	}

	
	@Override
	public Project getProject(String name) {
		return projects.get(name);
	}

	public void uploadProject(String projectName, File dir, User uploader, boolean force) throws ProjectManagerException {
		logger.info("Uploading files to " + projectName);
		Project project = projects.get(projectName);

		if (project == null) {
			throw new ProjectManagerException("Project not found.");
		}

		List<String> errors = new ArrayList<String>();
		DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);
		loader.loadProjectFlow(dir);
		errors.addAll(loader.getErrors());
		Map<String, Flow> flows = loader.getFlowMap();

		File projectPath = new File(projectDirectory, projectName);
		File installDir = new File(projectPath, FILE_DATE_FORMAT.print(System.currentTimeMillis()));
		
		if (!installDir.mkdir()) {
			throw new ProjectManagerException("Cannot create directory in " + projectDirectory);
		}

		for (Flow flow : flows.values()) {
			flow.setProjectId(projectName);
			try {
				if (flow.getErrors() != null) {
					errors.addAll(flow.getErrors());
				}
				flow.initialize();

				writeFlowFile(installDir, flow);
			}
			catch (IOException e) {
				throw new ProjectManagerException("Project directory "
						+ projectName + " cannot be created in "
						+ projectDirectory, e);
			}
		}

		File destDirectory = new File(installDir, PROJECT_DIRECTORY);
		dir.renameTo(destDirectory);

		// We install only if the project is not forced install or has no errors
		//if (force || errors.isEmpty()) {
		// We don't do force install any more
		if (errors.isEmpty()) {
			// We synchronize on project so that we don't collide when
			// uploading.
			synchronized (project) {
				logger.info("Uploading files to " + projectName);
				project.setSource(installDir.getName());
				project.setLastModifiedTimestamp(System.currentTimeMillis());
				project.setLastModifiedUser(uploader.getUserId());
				project.setFlows(flows);
			}

			try {
				writeProjectFile(projectPath, project);
			} 
			catch (IOException e) {
				throw new ProjectManagerException("Project directory "
						+ projectName + " cannot be created in "
						+ projectDirectory, e);
			}
		} 
		else {
			logger.info("Errors found loading project " + projectName);
			StringBuffer bufferErrors = new StringBuffer();
			for (String error : errors) {
				bufferErrors.append(error);
				bufferErrors.append("\n");
			}

			throw new ProjectManagerException(bufferErrors.toString());
		}
	}

	@Override
	public synchronized Project createProject(String projectName,
			String description, User creator) throws ProjectManagerException {
		if (projectName == null || projectName.trim().isEmpty()) {
			throw new ProjectManagerException("Project name cannot be empty.");
		} 
		else if (description == null || description.trim().isEmpty()) {
			throw new ProjectManagerException("Description cannot be empty.");
		} 
		else if (creator == null) {
			throw new ProjectManagerException("Valid creator user must be set.");
		} 
		else if (!projectName.matches("[a-zA-Z][a-zA-Z_0-9|-]*")) {
			throw new ProjectManagerException("Project names must start with a letter, followed by any number of letters, digits, '-' or '_'.");
		}

		if (projects.contains(projectName)) {
			throw new ProjectManagerException("Project already exists.");
		}

		File projectPath = new File(projectDirectory, projectName);
		if (projectPath.exists()) {
			throw new ProjectManagerException("Project already exists.");
		}

		if (!projectPath.mkdirs()) {
			throw new ProjectManagerException("Project directory " + projectName + " cannot be created in " + projectDirectory);
		}

		Permission perm = new Permission(Type.ADMIN);
		long time = System.currentTimeMillis();

		Project project = new Project(projectName);
		project.setUserPermission(creator.getUserId(), perm);
		project.setDescription(description);
		project.setCreateTimestamp(time);
		project.setLastModifiedTimestamp(time);
		project.setLastModifiedUser(creator.getUserId());

		logger.info("Trying to create " + project.getName() + " by user " + creator.getUserId());
		try {
			writeProjectFile(projectPath, project);
		} 
		catch (IOException e) {
			throw new ProjectManagerException("Project directory " + projectName + " cannot be created in " + projectDirectory, e);
		}
		projects.put(projectName, project);
		attachLoggerToProject(project);
		
		project.info("Project has been created by '" + creator.getUserId() + "'");
		
		return project;
	}

	private synchronized void writeProjectFile(File directory, Project project) throws IOException {
		Object object = project.toObject();
		File tmpFile = File.createTempFile("project-", ".json", directory);

		if (tmpFile.exists()) {
			tmpFile.delete();
		}

		logger.info("Writing project file " + tmpFile);
		String output = JSONUtils.toJSON(object, true);

		FileWriter writer = new FileWriter(tmpFile);
		try {
			writer.write(output);
		} 
		catch (IOException e) {
			if (writer != null) {
				writer.close();
			}

			throw e;
		}
		writer.close();

		File projectFile = new File(directory, PROPERTIES_FILENAME);
		File swapFile = new File(directory, PROPERTIES_FILENAME + "_old");

		projectFile.renameTo(swapFile);
		tmpFile.renameTo(projectFile);
		swapFile.delete();

	}

	private void writeFlowFile(File directory, Flow flow) throws IOException {
		Object object = flow.toObject();
		String filename = flow.getId() + FLOW_EXTENSION;
		File outputFile = new File(directory, filename);
		File oldOutputFile = new File(directory, filename + ".old");

		if (outputFile.exists()) {
			outputFile.renameTo(oldOutputFile);
		}

		logger.info("Writing flow file " + outputFile);
		String output = JSONUtils.toJSON(object, true);

		FileWriter writer = new FileWriter(outputFile);
		try {
			writer.write(output);
		} 
		catch (IOException e) {
			if (writer != null) {
				writer.close();
			}

			throw e;
		}
		writer.close();

		if (oldOutputFile.exists()) {
			oldOutputFile.delete();
		}
	}

	@Override
	public Props getProperties(String projectName, String source)
			throws ProjectManagerException {
		Project project = projects.get(projectName);
		if (project == null) {
			throw new ProjectManagerException("Project " + project + " cannot be found.");
		}

		return getProperties(project, source);
	}

	@Override
	public Props getProperties(Project project, String source)
			throws ProjectManagerException {

		String mySource = project.getName() + File.separatorChar
				+ project.getSource() + File.separatorChar + "src"
				+ File.separatorChar + source;
		Element sourceElement = sourceCache.get(mySource);

		if (sourceElement != null) {
			return Props.clone((Props) sourceElement.getObjectValue());
		}

		File file = new File(projectDirectory, mySource);
		if (!file.exists()) {
			throw new ProjectManagerException("Source file " + file.getAbsolutePath() + " doesn't exist.");
		}

		try {
			Props props = new Props((Props) null, file);
			return props;
		} 
		catch (IOException e) {
			throw new ProjectManagerException("Error loading file " + file.getPath(), e);
		}
	}

	@Override
	public synchronized Project removeProject(String projectName) throws ProjectManagerException {
		Project project = this.getProject(projectName);
		
		if (project == null) {
			throw new ProjectManagerException("Project " + projectName + " doesn't exist.");
		}
		
		File projectPath = new File(projectDirectory, projectName);
		File deletedProjectPath = new File(projectDirectory, DELETED_PROJECT_PREFIX + System.currentTimeMillis() + "." + projectName);
		if (projectPath.exists()) {
			if (!projectPath.renameTo(deletedProjectPath)) {
				throw new ProjectManagerException("Deleting of project failed.");
			}
		}

		projects.remove(projectName);
		return project;
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
	
	private static class PrefixFilter implements FileFilter {
		private String prefix;

		public PrefixFilter(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();

			return pathname.isFile() && !pathname.isHidden() && name.length() > prefix.length() && name.startsWith(prefix);
		}
	}

	@Override
	public void commitProject(String projectName) throws ProjectManagerException {
		Project project = projects.get(projectName);
		if (project == null) {
			throw new ProjectManagerException("Project " + projectName + " doesn't exist.");
		}

		File projectPath = new File(projectDirectory, projectName);
		try {
			writeProjectFile(projectPath, project);
		} 
		catch (IOException e) {
			throw new ProjectManagerException("Error committing project " + projectName, e);
		}
	}

	@Override
	public HashMap<String, Props> getAllFlowProperties(Project project, String flowId) throws ProjectManagerException {
		Flow flow = project.getFlow(flowId);
		if (flow == null) {
			throw new ProjectManagerException("Flow " + flowId + " doesn't exist in " + project.getName());
		}

		// Resolve all the node probs
		HashMap<String, Props> sourceMap = new HashMap<String, Props>();
		for (Node node : flow.getNodes()) {
			String source = node.getJobSource();
			Props props = getProperties(project, node.getJobSource());
			sourceMap.put(source, props);
		}

		// Resolve all the shared props.
		for(FlowProps flowProps: flow.getAllFlowProps().values()) {
			String source = flowProps.getSource();
			Props props = getProperties(project, source);
			sourceMap.put(source, props);
		}
		
		return sourceMap;
	}

	@Override
	public void copyProjectSourceFilesToDirectory(Project project, File directory) throws ProjectManagerException {
		
		if (!directory.exists()) {
			throw new ProjectManagerException("Destination directory " + directory + " doesn't exist.");
		}
		
		String mySource = project.getName() + File.separatorChar + project.getSource() + File.separatorChar + "src";
		
		File projectDir = new File(projectDirectory, mySource);
		if (!projectDir.exists()) {
			throw new ProjectManagerException("Project source directory " + mySource + " doesn't exist.");
		}

		logger.info("Copying from project dir " + projectDir + " to " + directory);
		try {
			FileUtils.copyDirectory(projectDir, directory);
		} catch (IOException e) {
			throw new ProjectManagerException(e.getMessage());
		}
	}

	@Override
	public void getProjectLogs(String projectId, long tailBytes, long skipBytes, Writer writer) throws IOException {
		File projectDir = new File(projectDirectory, projectId);
		
		if (!projectDir.exists()) {
			throw new IOException("Project directory " + projectDir + " doesn't exist.");
		}
		
		File logFile = new File(projectDir, projectLogFileName(projectId));
		if (!logFile.exists()) {
			throw new IOException("Project audit log for " + projectDir + " doesn't exist.");
		}
		
		long lookbackBytes = skipBytes + tailBytes;
		long fileLength = logFile.length();

		long skip = Math.max(0, fileLength - lookbackBytes);
		FileInputStream f = new FileInputStream(logFile);
		if (skip > 0) {
			f.skip(skip);
		}
		
		long bytesRead = 0;
		BufferedReader reader = new BufferedReader(new InputStreamReader(f));
		try {
			String line = reader.readLine();
			for (; line != null; line = reader.readLine()) {
				writer.write(line);
				writer.write("\n");
				bytesRead += line.length();
				
				// A very loose tail bytes count since it's by lines and encoding, but this is okay.
				if (bytesRead > tailBytes) {
					break;
				}
			}
		} finally {
			reader.close();
		}
	}
	
	private void attachLoggerToProject(Project project) {
		Logger logger = Logger.getLogger(".projectlogger." + project.getName());
		
		File projectPath = new File(projectDirectory, project.getName());
		
		String logName = projectLogFileName(project.getName());
		File logFile = new File(projectPath, logName);

		FileAppender appender = null;
		try {
			appender = new FileAppender(DEFAULT_LAYOUT, logFile.getPath());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		logger.addAppender(appender);
		
		project.attachLogger(logger);
	}
	
	private String projectLogFileName(String projectName) {
		return "_project." + projectName + ".log";
	}

}