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

package azkaban.webapp.servlet;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import azkaban.webapp.AzkabanWebServer;

/**
 * A ServletContextListener that loads the batch application
 */
public class AzkabanServletContextListener implements ServletContextListener {
  public static final String AZKABAN_SERVLET_CONTEXT_KEY = "azkaban_app";

  private AzkabanWebServer app;

  /**
   * Delete the app
   */
  public void contextDestroyed(ServletContextEvent event) {
    this.app = null;
  }

  /**
   * Load the app for use in non jetty containers.
   */
  public void contextInitialized(ServletContextEvent event) {
    try {
      this.app = new AzkabanWebServer();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    event.getServletContext().setAttribute(AZKABAN_SERVLET_CONTEXT_KEY,
        this.app);
  }
}
