/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.project;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.*;
import static java.util.Objects.*;


/**
 * Helper class to fetch an external dependency and prepare it as per specifications
 */
public class DependencyFetcher {
  private static final Logger log = Logger.getLogger(DependencyFetcher.class);
  private static final String TMP_FILENAME = "data.tmp";

  private final ProjectManagerConfig config;

  public DependencyFetcher(ProjectManagerConfig config) {
    this.config = config;
  }

  @VisibleForTesting
  void fetchDependency(URI uri) throws IOException {
    final Path tmpFilePath = tmpFilePath();

    // Download the resource
    fetchUri(uri, tmpFilePath);
    // key is the assumed to be unique and will be used to represent this data in Storage API
    String key = requireNonNull(computeHash(tmpFilePath));

    // Organize content
    Path target = Paths.get(tmpDir().toString(), key);
    if (Files.exists(target)) {
      log.debug(String.format("Already Cached. Skipping [URI: %s, KEY: %s]", uri, key));
      return;
    }

    Files.move(tmpFilePath, target);

    // Gather meta data
    Properties metadata = constructMetaData(uri, target);

    writeMetaData(metadata, key);
    log.info(String.format("Caching [URI: %s, KEY: %s]", uri, key));
  }

  private Path tmpDir() {
    return config.getFetchTmpDir();
  }

  private Path tmpFilePath() {
    return Paths.get(tmpDir().toString(), TMP_FILENAME);
  }

  private void writeMetaData(Properties metadata, String key) throws IOException {
    Path targetPropertiesPath = Paths.get(tmpDir().toString(), key + ".properties");
    OutputStream out = new FileOutputStream(targetPropertiesPath.toFile());
    metadata.store(out, "Metadata for key: " + key);
  }

  @VisibleForTesting
  Properties constructMetaData(URI uri, Path targetPath) throws MalformedURLException {
    Properties metaData = new Properties();
    metaData.setProperty("uri", uri.toString());
    metaData.setProperty("size", String.valueOf(targetPath.toFile().length()));
    String filename = retrieveFilename(uri);
    if (filename != null) {
      metaData.setProperty("filename", filename);
    }

    return metaData;
  }

  private String retrieveFilename(URI uri) throws MalformedURLException {
    return Paths.get(uri.toURL().getPath()).getFileName().toString();
  }

  @VisibleForTesting
  static String computeHash(Path targetPath) throws IOException {
    checkState(Files.exists(targetPath), "TMP_FILE does not exist!");
    return calcSHA1(targetPath.toFile());
  }

  /**
   * Read the file and calculate the SHA-1 checksum
   *
   * @param file
   *            the file to read
   * @return the hex representation of the SHA-1 using uppercase chars
   * @throws FileNotFoundException
   *             if the file does not exist, is a directory rather than a
   *             regular file, or for some other reason cannot be opened for
   *             reading
   * @throws IOException
   *             if an I/O error occurs
   * @throws NoSuchAlgorithmException
   *             should never happen
   */
  public static String calcSHA1(File file) throws IOException {
    /*
     * Most file systems are configured to use block sizes of 4096 or 8192.
     * http://stackoverflow.com/questions/236861/how-do-you-determine-the-ideal-buffer-size-when-using-fileinputstream
     */
    final int BUFFER_SIZE = 8192;
    MessageDigest sha1 = null;
    try {
      sha1 = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      // This is a weird exception. Probably due to a coding error. Escalate it here.
      Throwables.propagate(e);
    }
    requireNonNull(sha1);

    try (InputStream input = new FileInputStream(file)) {

      byte[] buffer = new byte[BUFFER_SIZE];
      int len = input.read(buffer);

      while (len != -1) {
        sha1.update(buffer, 0, len);
        len = input.read(buffer);
      }

      return new HexBinaryAdapter().marshal(sha1.digest());
    }
  }

  /**
   * Download a uri to a dest path of choice. Throws {@link RuntimeException}
   *
   * @param uri
   * @param dest
   * @return
   */
  public static long fetchUri(final URI uri, final Path dest) throws IOException {
    // TODO handle other types of URI. For now, assume everything is a URL
    return fetchUrl(uri.toURL(), dest);
  }

  /**
   * Download a url to a dest path of choice. Throws {@link RuntimeException}
   *
   * @param url url to download
   * @param dest destination file to be written to
   * @return Number of bytes written.
   *         -1 on failure
   */
  public static long fetchUrl(final URL url, final Path dest) throws IOException {
    long nBytesCopied;
    try (InputStream in = url.openStream()) {
      nBytesCopied = Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
    }
    return nBytesCopied;
  }
}
