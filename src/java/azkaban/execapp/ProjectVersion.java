package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;

import azkaban.utils.FileIOUtils;
import azkaban.utils.Utils;

public class ProjectVersion implements Comparable<ProjectVersion> {
	private final int projectId;
	private final int version;
	private File installedDir;
	
	public ProjectVersion(int projectId, int version) {
		this.projectId = projectId;
		this.version = version;
	}

	public int getProjectId() {
		return projectId;
	}

	public int getVersion() {
		return version;
	}
	
	public synchronized void setupProjectFiles(ProjectLoader projectLoader, File projectDir, Logger logger) throws ProjectManagerException, IOException {
		String projectVersion = String.valueOf(projectId) + "." + String.valueOf(version);
		if (installedDir == null) {
			installedDir = new File(projectDir, projectVersion);
		}
		
		if (!installedDir.exists()) {
			
			logger.info("First time executing new project. Setting up in directory " + installedDir.getPath());
			
			File tempDir = new File(projectDir, "_temp." + projectVersion + "." + System.currentTimeMillis());
			tempDir.mkdirs();
			ProjectFileHandler projectFileHandler = null;
			try {
				projectFileHandler = projectLoader.getUploadedFile(projectId, version);
				if ("zip".equals(projectFileHandler.getFileType())) {
					logger.info("Downloading zip file.");
					ZipFile zip = new ZipFile(projectFileHandler.getLocalFile());
					Utils.unzip(zip, tempDir);
	
					tempDir.renameTo(installedDir);
				}
				else {
					throw new IOException("The file type hasn't been decided yet.");
				}
			}
			finally {
				if (projectFileHandler != null) {
					projectFileHandler.deleteLocalFile();
				}
			}
		}
	}
	
	public synchronized void copyCreateSymlinkDirectory(File executionDir) throws IOException {
		if (installedDir == null || !installedDir.exists()) {
			throw new IOException("Installed dir doesn't exist");
		}
		else if (executionDir == null || !executionDir.exists()) {
			throw new IOException("Execution dir doesn't exist");
		}
		FileIOUtils.createDeepSymlink(installedDir, executionDir);
	}
	
	public synchronized void deleteDirectory() throws IOException {
		if (installedDir != null && installedDir.exists()) {
			FileUtils.deleteDirectory(installedDir);
		}
	}

	@Override
	public int compareTo(ProjectVersion o) {
		if (projectId == o.projectId) {
			return version - o.version;
		}
		
		return projectId - o.projectId;
	}
}