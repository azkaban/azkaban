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

package azkaban.reportal.util;

import java.io.IOException;
import java.io.OutputStream;

public class BoundedOutputStream extends OutputStream {
  OutputStream out;
  int totalCapacity;
  int remainingCapacity;
  boolean hasExceededSize = false;
  boolean havePrintedErrorMessage = false;

  public BoundedOutputStream(OutputStream out, int size) {
    this.out = out;
    this.totalCapacity = size;
    this.remainingCapacity = size;
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public void write(byte[] b) throws IOException {
    if (remainingCapacity <= 0) {
      hasExceededSize = true;
    } else if (remainingCapacity - b.length < 0) {
      out.write(b, 0, remainingCapacity);
      remainingCapacity = 0;
      hasExceededSize = true;
    } else {
      out.write(b);
      remainingCapacity -= b.length;
    }

    if (hasExceededSize && !havePrintedErrorMessage) {
      System.err.println("Output has exceeded the max limit of "
          + totalCapacity + " bytes. Truncating remaining output.");
      havePrintedErrorMessage = true;
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (remainingCapacity <= 0) {
      hasExceededSize = true;
    } else if (remainingCapacity - len < 0) {
      out.write(b, off, remainingCapacity);
      remainingCapacity = 0;
      hasExceededSize = true;
    } else {
      out.write(b, off, len);
      remainingCapacity -= len;
    }

    if (hasExceededSize && !havePrintedErrorMessage) {
      System.err.println("Output has exceeded the max limit of "
          + totalCapacity + " bytes. Truncating remaining output.");
      havePrintedErrorMessage = true;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (remainingCapacity <= 0) {
      hasExceededSize = true;

      if (!havePrintedErrorMessage) {
        System.err.println("Output has exceeded the max limit of "
            + totalCapacity + " bytes. Truncating remaining output.");
        havePrintedErrorMessage = true;
      }

      return;
    }
    out.write(b);
    remainingCapacity--;
  }

}
