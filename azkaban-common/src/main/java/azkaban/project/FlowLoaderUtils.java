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
import azkaban.executor.ExecutorTags;
import azkaban.flow.CommonJobProperties;
import azkaban.flow.Flow;
import azkaban.jobcallback.JobCallbackValidator;
import azkaban.project.validator.ValidationReport;
import azkaban.utils.MemConfValue;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;

/**
 * Utils to help load flows.
 */
public class FlowLoaderUtils {

  private static final Logger logger = LoggerFactory.getLogger(FlowLoaderUtils.class);
  private static final String XMS = "Xms";
  private static final String XMX = "Xmx";

  /**
   * Sets props in flow yaml file.
   *
   * @param path the flow or job path delimited by ":", e.g. "flow:subflow1:subflow2:job3"
   * @param flowFile the flow yaml file
   * @param prop the props to set
   */
  public static void setPropsInYamlFile(final String path, final File flowFile, final Props prop) {
    final DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(FlowStyle.BLOCK);
    final NodeBean nodeBean = FlowLoaderUtils.setPropsInNodeBean(path, flowFile, prop);
    try (final BufferedWriter writer = Files
        .newBufferedWriter(flowFile.toPath(), StandardCharsets.UTF_8)) {
      new Yaml(options).dump(nodeBean, writer);
    } catch (final IOException e) {
      throw new ProjectManagerException(
          "Failed to set properties in flow file " + flowFile.getName());
    }
  }

  /**
   * Sets props in node bean.
   *
   * @param path the flow or job path delimited by ":", e.g. "flow:subflow1:subflow2:job3"
   * @param flowFile the flow yaml file
   * @param prop the props to set
   * @return the node bean
   */
  public static NodeBean setPropsInNodeBean(final String path, final File flowFile,
      final Props prop) {
    final NodeBeanLoader loader = new NodeBeanLoader();
    try {
      final NodeBean nodeBean = loader.load(flowFile);
      final String[] pathList = path.split(Constants.PATH_DELIMITER);
      if (overridePropsInNodeBean(nodeBean, pathList, 0, prop)) {
        return nodeBean;
      } else {
        logger.error("Error setting props for " + path);
      }
    } catch (final Exception e) {
      logger.error("Failed to set props, error loading flow YAML file " + flowFile);
    }
    return null;
  }

  /**
   * Helper method to recursively find the node to override props.
   *
   * @param nodeBean the node bean
   * @param pathList the path list
   * @param idx the idx
   * @param prop the props to override
   * @return the boolean
   */
  private static boolean overridePropsInNodeBean(final NodeBean nodeBean, final String[] pathList,
      final int idx, final Props prop) {
    if (idx < pathList.length && nodeBean.getName().equals(pathList[idx])) {
      if (idx == pathList.length - 1) {
        if (prop.containsKey(Constants.NODE_TYPE)) {
          nodeBean.setType(prop.get(Constants.NODE_TYPE));
        }
        final Map<String, String> config = prop.getFlattened();
        config.remove(Constants.NODE_TYPE);
        nodeBean.setConfig(config);
        return true;
      }
      for (final NodeBean bean : nodeBean.getNodes()) {
        if (overridePropsInNodeBean(bean, pathList, idx + 1, prop)) {
          return true;
        }
      }
    }
    return false;
  }

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
    } catch (final Exception e) {
      logger.error("Failed to get props, error loading flow YAML file. ", e);
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
  private static boolean findPropsFromNodeBean(final NodeBean nodeBean,
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
    } catch (final Exception e) {
      logger.error("Failed to get flow trigger, error loading flow YAML file. ", e);
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
   * Adds required executor tags properties to a flow.
   *
   * @param flow the flow
   * @param props the props
   */
  public static void addRequiredExecutorTagsToFlow(final Flow flow, final Props props) {
    final ExecutorTags tags = ExecutorTags
        .getTagsFromProps(props, CommonJobProperties.REQUIRED_EXECUTOR_TAGS);
    flow.setRequiredExecutorTags(tags);
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

    final MemConfValue maxXms = MemConfValue.parseMaxXms(props);
    final MemConfValue maxXmx = MemConfValue.parseMaxXmx(props);

    for (final String jobName : jobPropsMap.keySet()) {
      final Props jobProps = jobPropsMap.get(jobName);
      final String xms = jobProps.getString(XMS, null);
      if (xms != null && !PropsUtils.isVariableReplacementPattern(xms)
          && Utils.parseMemString(xms) > maxXms.getSize()) {
        errors.add(String.format(
            "%s: Xms value has exceeded the allowed limit (max Xms = %s)",
            jobName, maxXms.getString()));
      }
      final String xmx = jobProps.getString(XMX, null);
      if (xmx != null && !PropsUtils.isVariableReplacementPattern(xmx)
          && Utils.parseMemString(xmx) > maxXmx.getSize()) {
        errors.add(String.format(
            "%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
            jobName, maxXmx.getString()));
      }

      // job callback properties check
      JobCallbackValidator.validate(jobName, props, jobProps, errors);
    }
  }

  /**
   * Clean up the directory.
   *
   * @param dir the directory to be deleted
   */
  public static void cleanUpDir(final File dir) {
    try {
      if (dir != null && dir.exists()) {
        FileUtils.deleteDirectory(dir);
      }
    } catch (final IOException e) {
      logger.error("Failed to delete the directory", e);
      dir.deleteOnExit();
    }
  }

  /**
   * Check if azkaban flow version is 2.0.
   *
   * @param azkabanFlowVersion the azkaban flow version
   * @return the boolean
   */
  public static boolean isAzkabanFlowVersion20(final double azkabanFlowVersion) {
    return Double.compare(azkabanFlowVersion, Constants.AZKABAN_FLOW_VERSION_2_0) == 0;
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

  /**
   * Implements Directory filter.
   */
  public static class DirFilter implements FileFilter {

    @Override
    public boolean accept(final File pathname) {
      return pathname.isDirectory();
    }
  }
}
