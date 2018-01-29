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

package azkaban.executor.mail;

import org.apache.log4j.Logger;

import java.util.HashMap;

public class MailCreatorRegistry {

  private static final Logger logger = Logger.getLogger(MailCreatorRegistry.class);
  private static final HashMap<String, MailCreator> registeredCreators = new HashMap<>();
  private static final MailCreator defaultCreator;
  private static MailCreator recommendedCreator;

  static {
    recommendedCreator = defaultCreator = new DefaultMailCreator();
    registerCreator(defaultCreator);
  }

  public static void registerCreator(final MailCreator creator) {
    logger.info("registering mail creator " + creator.getName());
    registeredCreators.put(creator.getName(), creator);
    recommendedCreator = creator;
  }

  public static MailCreator getRecommendedCreator() {
    return recommendedCreator;
  }

  public static MailCreator getCreator(final String name) {
    logger.info("retrieving mail creator " + name);
    MailCreator creator = registeredCreators.get(name);
    if (creator == null) {
      creator = defaultCreator;
    }
    return creator;
  }

}
