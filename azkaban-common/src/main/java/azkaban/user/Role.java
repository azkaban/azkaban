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

package azkaban.user;

public class Role {

  private final String name;
  private final Permission globalPermission;

  public Role(final String name, final Permission permission) {
    this.name = name;
    this.globalPermission = permission;
  }

  public Permission getPermission() {
    return this.globalPermission;
  }

  public String getName() {
    return this.name;
  }

  @Override
  public String toString() {
    return "Role " + this.name;
  }
}
