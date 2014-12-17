/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.metric.inmemoryemitter;

import java.util.List;

/**
 * Utility class for mathematical function of metric's history objects
 */
public final class InMemoryHistoryStatistics {

  /**
   * Returns the average
   * @param data
   * @return mean of data
   */
  public static double mean(final List<InMemoryHistoryNode> data) {
    double total = 0.0;
    for (InMemoryHistoryNode node : data) {
      total = total + ((Number)node.getValue()).doubleValue() ;
    }
    return total / data.size();
  }

  /**
   * Returns the sample standard deviation
   * @param data
   * @return standard deviation of data
   */
  public static double sdev(final List<InMemoryHistoryNode> data) {
    return Math.sqrt(variance(data));
  }


  /**
   * Returns the sample variance
   * @param data
   * @return variance of data
   */
  public static double variance(final List<InMemoryHistoryNode> data) {
    double mu = mean(data);
    double sumsq = 0.0;
    for (InMemoryHistoryNode node : data) {
      sumsq += sqr(mu - ((Number)node.getValue()).doubleValue());
    }
    return sumsq / data.size();
  }

  /**
   * Square of a number
   * @param x
   * @return x*x
   */
  public static double sqr(final double x) {
    return x * x;
  }
}
