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

import azkaban.utils.Utils;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.EscapeTool;

/**
 * A page to display
 */
public class Page {

  private static final String DEFAULT_MIME_TYPE = "text/html";
  private final HttpServletRequest request;
  private final HttpServletResponse response;
  private final VelocityEngine engine;
  private final VelocityContext context;
  private final String template;
  private String mimeType = DEFAULT_MIME_TYPE;

  /**
   * Creates a page and sets up the velocity engine to render
   */
  public Page(final HttpServletRequest request, final HttpServletResponse response,
      final VelocityEngine engine, final String template) {
    this.request = Utils.nonNull(request);
    this.response = Utils.nonNull(response);
    this.engine = Utils.nonNull(engine);
    this.template = Utils.nonNull(template);
    this.context = new VelocityContext();
    this.context.put("esc", new EscapeTool());
    this.context.put("session", request.getSession(true));
    this.context.put("context", request.getContextPath());
  }

  /**
   * Renders the page in UTF-8
   */
  public void render() {
    try {
      sanitizeContextContents();
      this.response.setHeader("Content-type", "text/html; charset=UTF-8");
      this.response.setCharacterEncoding("UTF-8");
      this.response.setContentType(this.mimeType);
      this.engine.mergeTemplate(this.template, "UTF-8", this.context, this.response.getWriter());
    } catch (final Exception e) {
      throw new PageRenderException(e);
    }
  }

  /**
   * Adds variables to the velocity context.
   */
  public void add(final String name, final Object value) {
    this.context.put(name, value);
  }

  private void sanitizeContextContents() {
    for(Object key: this.context.getKeys()) {
      Object value = this.context.get((String)key);
      if(key instanceof String && value instanceof String) {
        this.context.put((String)key, escapeHtmlExceptLineBreaks((String)value));
      }
    }
  }

  private static String escapeHtmlExceptLineBreaks(String value) {
    // Convert line breaks to \n
    String converted = value.replaceAll("<br/?>", "\n");
    // Escape the converted string
    String escaped = StringEscapeUtils.escapeHtml(converted);
    // Convert newlines back to <br>
    return escaped.replaceAll("\n", "<br>");
  }

  public void setMimeType(final String type) {
    this.mimeType = type;
  }
}
