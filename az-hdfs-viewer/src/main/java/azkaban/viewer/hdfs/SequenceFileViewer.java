/*
 * Copyright 2012 LinkedIn Corp.
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
 */
package azkaban.viewer.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;


public abstract class SequenceFileViewer extends HdfsFileViewer {

  protected abstract Set<Capability> getCapabilities(
      AzkabanSequenceFileReader.Reader reader);

  protected abstract void displaySequenceFile(
      AzkabanSequenceFileReader.Reader reader, PrintWriter output,
      int startLine, int endLine) throws IOException;

  @Override
  public Set<Capability> getCapabilities(final FileSystem fs, final Path path)
      throws AccessControlException {
    Set<Capability> result = EnumSet.noneOf(Capability.class);
    AzkabanSequenceFileReader.Reader reader = null;
    try {
      reader =
          new AzkabanSequenceFileReader.Reader(fs, path, new Configuration());
      result = getCapabilities(reader);
    } catch (final AccessControlException e) {
      throw e;
    } catch (final IOException e) {
      return EnumSet.noneOf(Capability.class);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (final IOException e) {
          e.printStackTrace();
        }
      }
    }

    return result;
  }

  @Override
  @SuppressWarnings("DefaultCharset")
  public void displayFile(final FileSystem fs, final Path file, final OutputStream outputStream,
      final int startLine, final int endLine) throws IOException {

    AzkabanSequenceFileReader.Reader reader = null;
    final PrintWriter writer = new PrintWriter(outputStream);
    try {
      reader =
          new AzkabanSequenceFileReader.Reader(fs, file, new Configuration());
      displaySequenceFile(reader, writer, startLine, endLine);
    } catch (final IOException e) {
      writer.write("Error opening sequence file " + e);
      throw e;
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
