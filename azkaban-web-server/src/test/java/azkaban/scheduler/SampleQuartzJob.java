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

import azkaban.utils.Props;
import java.io.Serializable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.JobExecutionContext;

public class SampleQuartzJob extends AbstractQuartzJob{

  public static final String DELEGATE_CLASS_NAME = "SampleService";
  public static int COUNT_EXECUTION = 0;
  private final SampleService sampleService;

  @Inject
  public SampleQuartzJob(final SampleService sampleService) {
    this.sampleService = sampleService;
  }

  @Override
  public void execute(final JobExecutionContext context) {
//    final SampleService service = asT(getKey(context, DELEGATE_CLASS_NAME), SampleService.class);
    COUNT_EXECUTION ++ ;
    this.sampleService.run();
  }
}

@Singleton
class SampleService implements Serializable{


  private final Props props;

  @Inject
  SampleService(final Props props) {
    this.props = props;
  }

  void run() {
    System.out.println(this.props.toString());
  }

  @Override
  public String toString() {
    return this.props.toString();
  }
}
