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

  public BoundedOutputStream(final OutputStream out, final int size) {
    this.out = out;
    this.totalCapacity = size;
    this.remainingCapacity = size;
  }

  @Override
  public void flush() throws IOException {
    this.out.flush();
  }

  @Override
  public void close() throws IOException {
    this.out.close();
  }

  @Override
  public void write(final byte[] b) throws IOException {
    if (this.remainingCapacity <= 0) {
      this.hasExceededSize = true;
    } else if (this.remainingCapacity - b.length < 0) {
      this.out.write(b, 0, this.remainingCapacity);
      this.remainingCapacity = 0;
      this.hasExceededSize = true;
    } else {
      this.out.write(b);
      this.remainingCapacity -= b.length;
    }

    if (this.hasExceededSize && !this.havePrintedErrorMessage) {
      System.err.println("Output has exceeded the max limit of "
          + this.totalCapacity + " bytes. Truncating remaining output.");
      this.havePrintedErrorMessage = true;
    }
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    if (this.remainingCapacity <= 0) {
      this.hasExceededSize = true;
    } else if (this.remainingCapacity - len < 0) {
      this.out.write(b, off, this.remainingCapacity);
      this.remainingCapacity = 0;
      this.hasExceededSize = true;
    } else {
      this.out.write(b, off, len);
      this.remainingCapacity -= len;
    }

    if (this.hasExceededSize && !this.havePrintedErrorMessage) {
      System.err.println("Output has exceeded the max limit of "
          + this.totalCapacity + " bytes. Truncating remaining output.");
      this.havePrintedErrorMessage = true;
    }
  }

  @Override
  public void write(final int b) throws IOException {
    if (this.remainingCapacity <= 0) {
      this.hasExceededSize = true;

      if (!this.havePrintedErrorMessage) {
        System.err.println("Output has exceeded the max limit of "
            + this.totalCapacity + " bytes. Truncating remaining output.");
        this.havePrintedErrorMessage = true;
      }

      return;
    }
    this.out.write(b);
    this.remainingCapacity--;
  }

}
