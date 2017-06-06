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

package azkaban.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class SplitterOutputStream extends OutputStream {

  List<OutputStream> outputs;

  public SplitterOutputStream(final OutputStream... outputs) {
    this.outputs = new ArrayList<>(outputs.length);
    for (final OutputStream output : outputs) {
      this.outputs.add(output);
    }
  }

  @Override
  public void write(final int b) throws IOException {
    for (final OutputStream output : this.outputs) {
      output.write(b);
    }
  }

  @Override
  public void write(final byte[] b) throws IOException {
    for (final OutputStream output : this.outputs) {
      output.write(b);
    }
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    for (final OutputStream output : this.outputs) {
      output.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    IOException exception = null;
    for (final OutputStream output : this.outputs) {
      try {
        output.flush();
      } catch (final IOException e) {
        exception = e;
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public void close() throws IOException {
    IOException exception = null;
    for (final OutputStream output : this.outputs) {
      try {
        output.close();
      } catch (final IOException e) {
        exception = e;
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

}
