/*
 * Copyright 2018 LinkedIn Corp.
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

package trigger.kafka;

import java.util.regex.Pattern;
import trigger.kafka.matcher.DependencyMatcher;


/**
 * A RegexKafkaDependencyMatcher implements the regex match for whole kafka payload.
 */
public class RegexKafkaDependencyMatcher implements DependencyMatcher<String> {
  private Pattern pattern ;
  RegexKafkaDependencyMatcher(Pattern _pattern) {
    this.pattern = _pattern;
  }

  @Override
  public boolean isMatch(String payload) {
    return pattern.matcher(payload).find();
  }
}
