/*
 * Copyright 2019 LinkedIn Corp.
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

import azkaban.utils.Props;
import com.sun.istack.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Executable Ramp Metadata. It is attached in the Flow's Executable Node
 * The Ramp Metadata is mainly includes the following Information
 * 1. ramp Props : which is a map of {dependency, dependencyPropValue}, for example, it looks like
 *    {"dali", {
 *       this: [{"jar:dali-data-pig", "...data-pig-9.2.10.jar"}, {"jar:dali-data-spark", "...data-spark-9.2.10.jar"}]
 *       parent: [{"jar:dali-data-pig", "...data-pig-9.2.1.jar"}, {"jar:dali-data-spark", "...data-spark-9.2.1.jar"}]
 *    }}
 * 2. ramp dependency map: which is a map to host the relationship between dependency and plugin types , for example,
 *    [
 *      {"jar:dali-data-pig", [pigLi, pig]},
 *      {"jar:dali-data-spark", [spark]}
 *    ]
 * 3. exceptional ramp items map: which hosts the exceptional items base on job name. It looks like
 *     {jobName_in_flow, List_of_Exceptional_Items_based_on_dependency}
 */
public class ExecutableFlowRampMetadata {

  private static String RAMP_PROP_KEY_PREFIX = "azkaban.ramp.";

  private Map<String, Props> rampPropsMap = new HashMap<>();
  private ExecutableRampDependencyMap executableRampDependencyMap = null;
  private Map<String, ExecutableRampExceptionalItems> exceptionalJobTreatments = new HashMap<>();

  private ExecutableFlowRampMetadata() {

  }

  public static ExecutableFlowRampMetadata createInstance(
      ExecutableRampDependencyMap executableRampDependencyMap,
      Map<String, ExecutableRampExceptionalItems> exceptionalJobTreatments) {
    ExecutableFlowRampMetadata executableFlowRampMetadata = new ExecutableFlowRampMetadata();
    executableFlowRampMetadata.executableRampDependencyMap = executableRampDependencyMap.clone();
    Map<String, ExecutableRampExceptionalItems> clonedExceptionalJobTreatments = new HashMap<>();
    clonedExceptionalJobTreatments.putAll(
        exceptionalJobTreatments.entrySet().stream().collect(Collectors.toMap(
            item -> item.getKey(),
            item -> item.getValue().clone()
        )));
    executableFlowRampMetadata.exceptionalJobTreatments = clonedExceptionalJobTreatments;
    return executableFlowRampMetadata;
  }

  public ExecutableFlowRampMetadata setRampProps(String ramp, Props props) {
    this.rampPropsMap.put(ramp, props);
    return this;
  }

  public String getRampItemValue(final String ramp, final String key) {
    return this.rampPropsMap.get(ramp).get(key);
  }


  public ExecutableFlowRampMetadata setExecutableRampDependencyMap(
      ExecutableRampDependencyMap executableRampDependencyMap) {
    this.executableRampDependencyMap = executableRampDependencyMap;
    return this;
  }

  public Map<String,ExecutableRampExceptionalItems> getExceptionalJobTreatments() {
    return exceptionalJobTreatments;
  }

  public ExecutableFlowRampMetadata setExceptionalJobTreatments(
      Map<String, ExecutableRampExceptionalItems> exceptionalJobTreatments) {
    this.exceptionalJobTreatments = exceptionalJobTreatments;
    return this;
  }

  public Set<String> getActiveRamps() {
    return this.rampPropsMap.entrySet()
        .stream()
        .filter(item ->
            ExecutableRampStatus.SELECTED.name()
                .equalsIgnoreCase(item.getValue().getSource()))
        .map(item -> item.getKey()).collect(Collectors.toSet());
  }

  /**
   * Select Ramp Props For Job based on jobId and jobType
   * @param jobId job_id which is the job name against flow
   * @param jobType job type
   * @return rampable dependency
   */

  synchronized public Props selectRampPropsForJob(@NotNull final String jobId, @NotNull final String jobType) {

    Props selectedProps = new Props();

    for(Map.Entry<String, Props> rampItem : rampPropsMap.entrySet()) {
      String rampId = rampItem.getKey();
      Props rampValue = rampItem.getValue();

      // 1. For each ramp, we need to check if there is any whitelist/blacklist treatment based on job_id,
      boolean isExceptionalJob = isExceptionalJob(rampId, jobId, rampValue.getSource());
      Props filteredProps = generateFilteredProps(rampValue, isExceptionalJob);

      // 2. Continue Filter By jobType
      Set<String> filteredDependencies
          = filteredProps
          .getKeySet()
          .stream()
          .filter(dependency -> executableRampDependencyMap.isValidJobType(dependency, jobType))
          .collect(Collectors.toSet());

      // 3. Append the dependency into the final consolidated props.
      for(String dependency : filteredDependencies) {
        selectedProps.put(
            RAMP_PROP_KEY_PREFIX + dependency,
            Optional.ofNullable(filteredProps.get(dependency))
                .orElse(executableRampDependencyMap.getDefaultValue(dependency))
        );
      }
    }
    return selectedProps;
  }

  synchronized private boolean isExceptionalJob(final String rampId, final String jobId, final String source) {
    ExecutableRampExceptionalItems exceptionalJobs = exceptionalJobTreatments.get(rampId);
    if (exceptionalJobs == null) {
      return false;
    }
    switch (exceptionalJobs.getStatus(jobId)) {
      case WHITELISTED:
        return ExecutableRampStatus.of(source).equals(ExecutableRampStatus.UNSELECTED);
      case BLACKLISTED:
        return ExecutableRampStatus.of(source).equals(ExecutableRampStatus.SELECTED);
      default: // by default it means no exceptional ramp treatment for this job
        return false;
    }
  }

  synchronized private Props generateFilteredProps(Props props, boolean isExceptionalJob) {
    Props filteredProps = new Props();
    Set<String> keySet = props.getKeySet().stream().collect(Collectors.toSet());
    for (String key : keySet) {
     if (!props.get(key).isEmpty()) {
       String currentValue = props.get(key);
       String parentValue = Optional.ofNullable(props.getParent()).map(prop -> prop.get(key)).orElse("");
       filteredProps.put(key, (isExceptionalJob && !parentValue.isEmpty()) ? parentValue : currentValue);
     }
    }
    return filteredProps;
 }
}
