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

package azkaban.executor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

public class SleepJavaJob {
  private boolean fail;
  private String seconds;
  private int attempts;
  private int currentAttempt;

  public SleepJavaJob(String id, Properties props) {
    setup(props);
  }

  public SleepJavaJob(String id, Map<String, String> parameters) {
    Properties properties = new Properties();
    properties.putAll(parameters);

    setup(properties);
  }

  private void setup(Properties props) {
    String failStr = (String) props.get("fail");

    if (failStr == null || failStr.equals("false")) {
      fail = false;
    } else {
      fail = true;
    }

    currentAttempt =
        props.containsKey("azkaban.job.attempt") ? Integer
            .parseInt((String) props.get("azkaban.job.attempt")) : 0;
    String attemptString = (String) props.get("passRetry");
    if (attemptString == null) {
      attempts = -1;
    } else {
      attempts = Integer.valueOf(attemptString);
    }
    seconds = (String) props.get("seconds");

    if (fail) {
      System.out.println("Planning to fail after " + seconds
          + " seconds. Attempts left " + currentAttempt + " of " + attempts);
    } else {
      System.out.println("Planning to succeed after " + seconds + " seconds.");
    }
  }

  public static void main(String[] args) throws Exception {
    String propsFile = System.getenv("JOB_PROP_FILE");
    Properties prop = new Properties();
    prop.load(new BufferedReader(new FileReader(propsFile)));

    String jobName = System.getenv("JOB_NAME");
    SleepJavaJob job = new SleepJavaJob(jobName, prop);

    job.run();
  }

  public void run() throws Exception {
    if (seconds == null) {
      throw new RuntimeException("Seconds not set");
    }

    int sec = Integer.parseInt(seconds);
    System.out.println("Sec " + sec);
    synchronized (this) {
      try {
        this.wait(sec * 1000);
      } catch (InterruptedException e) {
        System.out.println("Interrupted " + fail);
      }
    }

    if (fail) {
      if (attempts <= 0 || currentAttempt <= attempts) {
        throw new Exception("I failed because I had to.");
      }
    }
  }

  public void cancel() throws Exception {
    System.out.println("Cancelled called on Sleep job");
    fail = true;
    synchronized (this) {
      this.notifyAll();
    }
  }

}
