/*
 * Copyright 2022 LinkedIn Corp.
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
package azkaban.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A thread factory that sets the threads to run as daemons. (Otherwise things
 * that embed the threadpool can't shut themselves down).
 */
public class DaemonThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber;
  private final String namePrefix;

  public DaemonThreadFactory(String threadNamePrefix) {
    this.threadNumber = new AtomicInteger(1);
    this.namePrefix = threadNamePrefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(r, this.namePrefix + "-" + this.threadNumber.getAndIncrement());
    t.setDaemon(true);
    return t;
  }

}
