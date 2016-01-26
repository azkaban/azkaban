package azkaban.webapp.servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.mortbay.io.WriterOutputStream;

import azkaban.server.AzkabanServer;

public class FileSystemService {

	private final String basePath = AzkabanServer.getAzkabanProperties().get("azkaban.project.dir");

	public void streamResource(String target, HttpServletResponse response) throws IOException, ServletException {

		File resource = new File(basePath + target);
		OutputStream out = null;
		try {
			out = response.getOutputStream();
		} catch (IllegalStateException e) {
			out = new WriterOutputStream(response.getWriter());
		}

		if (resource.exists()) {
			if (resource.isDirectory()) {
				String directoryContent = getDirectoryContent(target);
				response.setContentType("text/html");
				response.setContentLength(directoryContent.length());
				response.setStatus(HttpServletResponse.SC_OK);
				out.write(directoryContent.toString().getBytes());
			} else {
				response.setContentType("text/plain");
				response.setStatus(HttpServletResponse.SC_OK);
				int length = streamFileContent(target, out);
				response.setContentLength(length);
			}
		}
	}

	private String getDirectoryContent(String target) {
		StringBuffer directoryContent = new StringBuffer();
		Collection<File> files = FileUtils.listFilesAndDirs(new File(basePath + target), TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE);
		directoryContent.append("<table border=0>");

		if (!"/".equals(target) && target != null && target.length() > 0) {
			File parent = new File(basePath + target).getParentFile();
			directoryContent.append("<tr><td><a href=\"#\" onclick=\"browseDirectory('"
					+ parent.getAbsolutePath().substring(basePath.length()) + "/')\">[Parent Folder]</td></tr>");
		}

		for (File file : files) {
			if (file.getParent().equals(new File(basePath + target).getPath())) {
				if (file.isDirectory()) {
					directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"browseDirectory('"
							+ target + file.getName() + "/')\">" + file.getName() + "/</td></tr>");
				} else {
					directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"fetchFileContent('"
							+ target + file.getName() + "')\">" + file.getName() + "</td></tr>");
				}
			}
		}
		directoryContent.append("</table><br>");
		return directoryContent.toString();
	}

	private int streamFileContent(String target, OutputStream out) throws IOException {
		int len, totalLength = 0;
		FileInputStream in = null;
		try {
			in = new FileInputStream(basePath + target);
			int bufferSize = 2 * 8192;
			byte buffer[] = new byte[bufferSize];

			while (true) {
				len = in.read(buffer, 0, bufferSize);
				totalLength += len;
				if (len < 0)
					break;
				out.write(buffer, 0, len);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		return totalLength + 1;
	}

	public void saveResource(String target, String content) throws IOException, ServletException {
		OutputStream out = null;
		try {
			File resource = new File(basePath + target);
			if (resource.exists() && resource.isFile()) {
				out = new FileOutputStream(resource);
				out.write((content + "").getBytes());
			}
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}
}
