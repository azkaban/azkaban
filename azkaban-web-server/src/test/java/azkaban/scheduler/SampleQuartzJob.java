/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.scheduler;

import java.io.Serializable;
import org.quartz.JobExecutionContext;

public class SampleQuartzJob extends AbstractQuartzJob{

  public static final String DELEGATE_CLASS_NAME = "SampleService";
  public static int COUNT_EXECUTION = 0;

  public SampleQuartzJob() {
  }

  @Override
  public void execute(final JobExecutionContext context) {
    final SampleService service = asT(getKey(context, DELEGATE_CLASS_NAME), SampleService.class);
    COUNT_EXECUTION ++ ;
    service.run();
  }
}

class SampleService implements Serializable{

  private final String field1;
  private final String field2;

  SampleService(final String field1, final String field2) {
    this.field1 = field1;
    this.field2 = field2;
  }

  void run() {
    System.out.println("field1: " + this.field1 + "==== field2: " + this.field2);
  }

  @Override
  public String toString() {
    return "field1: " + this.field1 + ", field2: " + this.field2;
  }
}
