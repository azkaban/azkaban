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

import azkaban.flow.CommonJobProperties;
import azkaban.flow.Flow;
import azkaban.project.validator.ValidationReport;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileFilter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FlowLoaderUtils {

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
