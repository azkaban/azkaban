/*
 * Copyright 2018 LinkedIn Corp.
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
 *
 * THIS IS AN EXPERIMENTAL FEATURE, USE WITH CAUTION.
 *
 * This viewer is aimed to support the rendering of very basic html files.
 * The content of a html file will be rendered inside an iframe on azkaban
 * web page to protect from possible malicious javascript code. It does not
 * support rendering local image files (e.g. image stored on hdfs), but it
 * does support showing images stored on remote network locations.
 *
 * In fact, not just images, but any data that is stored on HDFS are not
 * accessible from the html page, for example, css and js files. Everything
 * must either be self contained or referenced with internet location.
 * (e.g. jquery script hosted on google.com can be fetched, but jquery script
 * stored on local hdfs cannot)
 */
package azkaban.viewer.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HtmlFileViewer extends HdfsFileViewer {

  private static final Logger LOG = LoggerFactory.getLogger(HtmlFileViewer.class);
  // only display the first 25M chars. it is used to prevent
  // showing/downloading gb of data
  private static final int BUFFER_LIMIT = 25000000;
  private static final String VIEWER_NAME = "Html";

  private final Set<String> acceptedSuffix = new HashSet<>();

  public HtmlFileViewer() {
    this.acceptedSuffix.add(".htm");
    this.acceptedSuffix.add(".html");
  }

  @Override
  public String getName() {
    return VIEWER_NAME;
  }


  @Override
  public Set<Capability> getCapabilities(final FileSystem fs, final Path path) {
    final String fileName = path.getName();
    final int pos = fileName.lastIndexOf('.');
    if (pos < 0) {
      return EnumSet.noneOf(Capability.class);
    }

    final String suffix = fileName.substring(pos).toLowerCase();
    if (this.acceptedSuffix.contains(suffix)) {
      return EnumSet.of(Capability.READ);
    } else {
      return EnumSet.noneOf(Capability.class);
    }
  }

  @Override
  public void displayFile(final FileSystem fs, final Path path, final OutputStream outputStream,
      final int startLine, final int endLine) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("read in uncompressed html file");
    }

    // BUFFER_LIMIT is the only thing we care about, line limit is redundant and actually not
    // very useful for html files. Thus using Integer.MAX_VALUE to effectively remove the endLine limit.
    TextFileViewer.displayFileContent(
        fs, path, outputStream, startLine, Integer.MAX_VALUE, BUFFER_LIMIT
    );
  }

  @Override
  public ContentType getContentType() {
    return ContentType.HTML;
  }
}
