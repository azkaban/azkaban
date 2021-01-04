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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class SleepJavaJob {

  private boolean fail;
  private String seconds;
  private int attempts;
  private int currentAttempt;

  public SleepJavaJob(final String id, final Properties props) {
    setup(props);
  }

  public SleepJavaJob(final String id, final Map<String, String> parameters) {
    final Properties properties = new Properties();
    properties.putAll(parameters);

    setup(properties);
  }

  public static void main(final String[] args) throws Exception {
    final String propsFile = System.getenv("JOB_PROP_FILE");
    final Properties prop = new Properties();
    prop.load(Files.newBufferedReader(Paths.get(propsFile), StandardCharsets.UTF_8));

    final String jobName = System.getenv("JOB_NAME");
    final SleepJavaJob job = new SleepJavaJob(jobName, prop);

    job.run();
  }

  private void setup(final Properties props) {
    final String failStr = (String) props.get("fail");

    if (failStr == null || failStr.equals("false")) {
      this.fail = false;
    } else {
      this.fail = true;
    }

    this.currentAttempt =
        props.containsKey("azkaban.job.attempt") ? Integer
            .parseInt((String) props.get("azkaban.job.attempt")) : 0;
    final String attemptString = (String) props.get("passRetry");
    if (attemptString == null) {
      this.attempts = -1;
    } else {
      this.attempts = Integer.valueOf(attemptString);
    }
    this.seconds = (String) props.get("seconds");

    if (this.fail) {
      System.out.println("Planning to fail after " + this.seconds
          + " seconds. Attempts left " + this.currentAttempt + " of " + this.attempts);
    } else {
      System.out.println("Planning to succeed after " + this.seconds + " seconds.");
    }
  }

  public void run() throws Exception {
    if (this.seconds == null) {
      throw new RuntimeException("Seconds not set");
    }

    final int sec = Integer.parseInt(this.seconds);
    System.out.println("Sec " + sec);
    synchronized (this) {
      try {
        if (sec > 0) {
          this.wait(sec * 1000);
        }
      } catch (final InterruptedException e) {
        System.out.println("Interrupted " + this.fail);
      }
    }

    if (this.fail) {
      if (this.attempts <= 0 || this.currentAttempt <= this.attempts) {
        throw new Exception("I failed because I had to.");
      }
    }
  }

  public void cancel() throws Exception {
    System.out.println("Cancelled called on Sleep job");
    this.fail = true;
    synchronized (this) {
      this.notifyAll();
    }
  }

}
