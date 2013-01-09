package azkaban.project;

import java.io.File;

public class ProjectFileHandler {
	private final int projectId;
	private final int version;
	private final long uploadTime;
	private String fileType;
	private String fileName;
	private String uploader;
	private byte[] md5Hash;
	private int numChunks;
	private File localFile = null;

	public ProjectFileHandler(
			int projectId, 
			int version, 
			long uploadTime,
			String uploader,
			String fileType, 
			String fileName,
			int numChunks,
			byte[] md5Hash) 
	{
		this.projectId = projectId;
		this.version = version;
		this.uploadTime = uploadTime;
		this.setUploader(uploader);
		this.setFileType(fileType);
		this.setFileName(fileName);
		this.setMd5Hash(md5Hash);
		this.setNumChunks(numChunks);
	}

	public int getProjectId() {
		return projectId;
	}

	public int getVersion() {
		return version;
	}

	public long getUploadTime() {
		return uploadTime;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public byte[] getMd5Hash() {
		return md5Hash;
	}

	public void setMd5Hash(byte[] md5Hash) {
		this.md5Hash = md5Hash;
	}

	public File getLocalFile() {
		return localFile;
	}

	public synchronized void setLocalFile(File localFile) {
		this.localFile = localFile;
	}
	
	public synchronized void deleteLocalFile() {
		if (localFile == null) {
			return;
		}
		else {
			localFile.delete();
			localFile = null;
		}
	}

	public String getUploader() {
		return uploader;
	}

	public void setUploader(String uploader) {
		this.uploader = uploader;
	}

	public int getNumChunks() {
		return numChunks;
	}

	public void setNumChunks(int numChunks) {
		this.numChunks = numChunks;
	}

}