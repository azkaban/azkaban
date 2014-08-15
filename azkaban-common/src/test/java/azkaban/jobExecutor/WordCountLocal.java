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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import azkaban.utils.Props;

public class WordCountLocal extends AbstractJob {

  private String _input = null;
  private String _output = null;
  private Map<String, Integer> _dic = new HashMap<String, Integer>();

  public static void main(String[] args) throws Exception {
    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    System.out.println("propsFile: " + propsFile);
    Props prop = new Props(null, propsFile);
    WordCountLocal instance = new WordCountLocal("", prop);
    instance.run();
  }

  public WordCountLocal(String id, Props prop) {
    super(id, Logger.getLogger(WordCountLocal.class));
    _input = prop.getString("input");
    _output = prop.getString("output");
  }

  public void run() throws Exception {

    if (_input == null)
      throw new Exception("input file is null");
    if (_output == null)
      throw new Exception("output file is null");
    BufferedReader in =
        new BufferedReader(new InputStreamReader(new FileInputStream(_input)));

    String line = null;
    while ((line = in.readLine()) != null) {
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken();

        if (word.toString().equals("end_here")) { // expect an out-of-bound
                                                  // exception
          String[] errArray = new String[1];
          System.out.println("string in possition 2 is " + errArray[1]);
        }

        if (_dic.containsKey(word)) {
          Integer num = _dic.get(word);
          _dic.put(word, num + 1);
        } else {
          _dic.put(word, 1);
        }
      }
    }
    in.close();

    PrintWriter out = new PrintWriter(new FileOutputStream(_output));
    for (Map.Entry<String, Integer> entry : _dic.entrySet()) {
      out.println(entry.getKey() + "\t" + entry.getValue());
    }
    out.close();
  }

  @Override
  public Props getJobGeneratedProperties() {
    return new Props();
  }

  @Override
  public boolean isCanceled() {
    return false;
  }

}
