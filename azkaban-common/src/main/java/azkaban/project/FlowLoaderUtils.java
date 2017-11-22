/*
* Copyright 2017 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.project;

import azkaban.Constants;
import azkaban.flow.CommonJobProperties;
import azkaban.flow.Flow;
import azkaban.jobcallback.JobCallbackValidator;
import azkaban.project.validator.ValidationReport;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils to help load flows.
 */
public class FlowLoaderUtils {

  private static final Logger logger = LoggerFactory.getLogger(FlowLoaderUtils.class);
  private static final String XMS = "Xms";
  private static final String XMX = "Xmx";
  /**
   * Gets flow or job props from flow yaml file.
   *
   * @param path the flow or job path delimited by ":", e.g. "flow:subflow1:subflow2:job3"
   * @param flowFile the flow yaml file
   * @return the props from yaml file
   */
  public static Props getPropsFromYamlFile(final String path, final File flowFile) {
    final List<Props> propsList = new ArrayList<>();
    final NodeBeanLoader loader = new NodeBeanLoader();

    try {
      final NodeBean nodeBean = loader.load(flowFile);
      final String[] pathList = path.split(Constants.PATH_DELIMITER);
      if (findPropsFromNodeBean(nodeBean, pathList, 0, propsList)) {
        if (!propsList.isEmpty()) {
          return propsList.get(0);
        } else {
          logger.error("Error getting props for " + path);
        }
      }
    } catch (final FileNotFoundException e) {
      logger.error("Failed to get props, error loading flow YAML file " + flowFile);
    }
    return null;
  }

  /**
   * Helper method to recursively find props from node bean.
   *
   * @param nodeBean the node bean
   * @param pathList the path list
   * @param idx the idx
   * @param propsList the props list
   * @return the boolean
   */
  public static boolean findPropsFromNodeBean(final NodeBean nodeBean,
      final String[] pathList, final int idx, final List<Props> propsList) {
    if (idx < pathList.length && nodeBean.getName().equals(pathList[idx])) {
      if (idx == pathList.length - 1) {
        propsList.add(nodeBean.getProps());
        return true;
      }
      for (final NodeBean bean : nodeBean.getNodes()) {
        if (findPropsFromNodeBean(bean, pathList, idx + 1, propsList)) {
          return true;
        }
      }
    }
    return false;
  }

  public static FlowTrigger getFlowTriggerFromYamlFile(final File flowFile) {
    final NodeBeanLoader loader = new NodeBeanLoader();
    try {
      final NodeBean nodeBean = loader.load(flowFile);
      return loader.toFlowTrigger(nodeBean.getTrigger());
    } catch (final FileNotFoundException e) {
      logger.error("Failed to get flow trigger, error loading flow YAML file " + flowFile);
    }
    return null;
  }

  /**
   * Adds email properties to a flow.
   *
   * @param flow the flow
   * @param prop the prop
   */
  public static void addEmailPropsToFlow(final Flow flow, final Props prop) {
    final List<String> successEmailList =
        prop.getStringList(CommonJobProperties.SUCCESS_EMAILS,
            Collections.EMPTY_LIST);
    final Set<String> successEmail = new HashSet<>();
    for (final String email : successEmailList) {
      successEmail.add(email.toLowerCase());
    }

    final List<String> failureEmailList =
        prop.getStringList(CommonJobProperties.FAILURE_EMAILS,
            Collections.EMPTY_LIST);
    final Set<String> failureEmail = new HashSet<>();
    for (final String email : failureEmailList) {
      failureEmail.add(email.toLowerCase());
    }

    final List<String> notifyEmailList =
        prop.getStringList(CommonJobProperties.NOTIFY_EMAILS,
            Collections.EMPTY_LIST);
    for (String email : notifyEmailList) {
      email = email.toLowerCase();
      successEmail.add(email);
      failureEmail.add(email);
    }

    flow.addFailureEmails(failureEmail);
    flow.addSuccessEmails(successEmail);
  }

  /**
   * Generate flow loader report validation report.
   *
   * @param errors the errors
   * @return the validation report
   */
  public static ValidationReport generateFlowLoaderReport(final Set<String> errors) {
    final ValidationReport report = new ValidationReport();
    report.addErrorMsgs(errors);
    return report;
  }

  /**
   * Check job properties.
   *
   * @param projectId the project id
   * @param props the server props
   * @param jobPropsMap the job props map
   * @param errors the errors
   */
  public static void checkJobProperties(final int projectId, final Props props,
      final Map<String, Props> jobPropsMap, final Set<String> errors) {
    // if project is in the memory check whitelist, then we don't need to check
    // its memory settings
    if (ProjectWhitelist.isProjectWhitelisted(projectId,
        ProjectWhitelist.WhitelistType.MemoryCheck)) {
      return;
    }

    final String maxXms = props.getString(
        Constants.JobProperties.JOB_MAX_XMS, Constants.JobProperties.MAX_XMS_DEFAULT);
    final String maxXmx = props.getString(
        Constants.JobProperties.JOB_MAX_XMX, Constants.JobProperties.MAX_XMX_DEFAULT);
    final long sizeMaxXms = Utils.parseMemString(maxXms);
    final long sizeMaxXmx = Utils.parseMemString(maxXmx);

    for (final String jobName : jobPropsMap.keySet()) {
      final Props jobProps = jobPropsMap.get(jobName);
      final String xms = jobProps.getString(XMS, null);
      if (xms != null && !PropsUtils.isVarialbeReplacementPattern(xms)
          && Utils.parseMemString(xms) > sizeMaxXms) {
        errors.add(String.format(
            "%s: Xms value has exceeded the allowed limit (max Xms = %s)",
            jobName, maxXms));
      }
      final String xmx = jobProps.getString(XMX, null);
      if (xmx != null && !PropsUtils.isVarialbeReplacementPattern(xmx)
          && Utils.parseMemString(xmx) > sizeMaxXmx) {
        errors.add(String.format(
            "%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
            jobName, maxXmx));
      }

      // job callback properties check
      JobCallbackValidator.validate(jobName, props, jobProps, errors);
    }
  }

  /**
   * Implements Suffix filter.
   */
  public static class SuffixFilter implements FileFilter {

    private final String suffix;

    /**
     * Instantiates a new Suffix filter.
     *
     * @param suffix the suffix
     */
    public SuffixFilter(final String suffix) {
      this.suffix = suffix;
    }

    @Override
    public boolean accept(final File pathname) {
      final String name = pathname.getName();

      return pathname.isFile() && !pathname.isHidden()
          && name.length() > this.suffix.length() && name.endsWith(this.suffix);
    }
  }
}
