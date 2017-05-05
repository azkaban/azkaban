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

  private String input = null;
  private String output = null;
  private Map<String, Integer> dict = new HashMap<>();

  public static void main(String[] args) throws Exception {
    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    System.out.println("propsFile: " + propsFile);
    Props prop = new Props(null, propsFile);
    WordCountLocal instance = new WordCountLocal("", prop);
    instance.run();
  }

  private WordCountLocal(String id, Props prop) {
    super(id, Logger.getLogger(WordCountLocal.class));
    input = prop.getString("input");
    output = prop.getString("output");
  }

  @Override
  public void run() throws Exception {

    if (input == null) {
      throw new Exception("input file is null");
    }
    if (output == null) {
      throw new Exception("output file is null");
    }
    List<String> lines = Files.readAllLines(Paths.get(input), StandardCharsets.UTF_8);
    for (String line : lines) {
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken();

        if (word.equals("end_here")) { // expect an out-of-bound
          // exception
          // todo HappyRay: investigate what the following statements are designed to do.
          String[] errArray = new String[1];
          System.out.println("string in possition 2 is " + errArray[1]);
        }

        if (dict.containsKey(word)) {
          Integer num = dict.get(word);
          dict.put(word, num + 1);
        } else {
          dict.put(word, 1);
        }
      }
    }

    try (PrintWriter out = new PrintWriter(output, StandardCharsets.UTF_8.toString())) {
      for (Map.Entry<String, Integer> entry : dict.entrySet()) {
        out.println(entry.getKey() + "\t" + entry.getValue());
      }
    }
  }
}
