/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.jobExecutor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Utils {
  private Utils() {
  }

  public static void dumpFile(String filename, String filecontent)
      throws IOException {
    PrintWriter writer = new PrintWriter(new FileWriter(filename));
    writer.print(filecontent);
    writer.close();
  }

  public static void removeFile(String filename) {
    File file = new File(filename);
    file.delete();
  }
}
