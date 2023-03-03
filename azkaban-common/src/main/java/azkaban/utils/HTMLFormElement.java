/*
 * Copyright 2023 LinkedIn Corp.
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

/**
 * Represents an HTML form element. It's used to generate HTML dynamically. For example, custom
 * alerter plugins specify their UI parameters as a list of these objects. That list is then
 * processed when rendering the SLA definition page to generate form elements.
 */
public class HTMLFormElement {

  public enum HTMLFormElementType {
    TEXTAREA,
    INPUT_TEXT,
    INPUT_CHECKBOX,
    SELECT
  }

  private String label;
  private final String name;
  private final HTMLFormElementType type;
  private final String defaultValue;

  private HTMLFormElement(final String label, final String name, final HTMLFormElementType type,
      final String defaultValue) {
    this.label = label;
    this.name = name;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public String getName() {
    return this.name;
  }

  public String getLabel() {
    return this.label;
  }

  public String getDefaultValue() {
    return this.defaultValue;
  }

  public HTMLFormElementType getType() {
    return this.type;
  }

  public static class HTMLFormElementBuilder {

    private String label;
    private String name;
    private HTMLFormElementType type;
    private String defaultValue = null;

    public HTMLFormElementBuilder(final String label, final String name,
        final HTMLFormElementType type) {
      this.label = label;
      this.name = name;
      this.type = type;
    }

    public HTMLFormElementBuilder setLabel(final String label) {
      this.label = label;
      return this;
    }

    public HTMLFormElementBuilder setName(final String name) {
      this.name = name;
      return this;
    }

    public HTMLFormElementBuilder setType(final HTMLFormElementType type) {
      this.type = type;
      return this;
    }

    public HTMLFormElementBuilder setDefaultValue(final String defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public HTMLFormElement createHTMLFormElement() {
      return new HTMLFormElement(this.label, this.name, this.type, this.defaultValue);
    }
  }
}

