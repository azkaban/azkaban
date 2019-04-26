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

import azkaban.utils.Props;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;


public class WordCountLocal extends AbstractJob {

  private final Map<String, Integer> dict = new HashMap<>();
  private String input = null;
  private String output = null;

  private WordCountLocal(final String id, final Props prop) {
    super(id, Logger.getLogger(WordCountLocal.class));
    this.input = prop.getString("input");
    this.output = prop.getString("output");
  }

  public static void main(final String[] args) throws Exception {
    final String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    System.out.println("propsFile: " + propsFile);
    final Props prop = new Props(null, propsFile);
    final WordCountLocal instance = new WordCountLocal("", prop);
    instance.run();
  }

  @Override
  public void run() throws Exception {

    if (this.input == null) {
      throw new Exception("input file is null");
    }
    if (this.output == null) {
      throw new Exception("output file is null");
    }
    final List<String> lines = Files.readAllLines(Paths.get(this.input), StandardCharsets.UTF_8);
    for (final String line : lines) {
      final StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        final String word = tokenizer.nextToken();

        if (word.equals("end_here")) { // expect an out-of-bound
          // exception
          // todo HappyRay: investigate what the following statements are designed to do.
          final String[] errArray = new String[1];
          System.out.println("string in possition 2 is " + errArray[1]);
        }

        if (this.dict.containsKey(word)) {
          final Integer num = this.dict.get(word);
          this.dict.put(word, num + 1);
        } else {
          this.dict.put(word, 1);
        }
      }
    }

    try (PrintWriter out = new PrintWriter(this.output, StandardCharsets.UTF_8.toString())) {
      for (final Map.Entry<String, Integer> entry : this.dict.entrySet()) {
        out.println(entry.getKey() + "\t" + entry.getValue());
      }
    }
  }
}
