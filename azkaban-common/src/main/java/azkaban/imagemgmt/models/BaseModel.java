/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.imagemgmt.models;

/**
 * Base Model class representing id and audit information such as created by user, created
 * timestamp, modified by user, modified timestamp etc.
 */
public abstract class BaseModel {
  // Id of the record
  protected int id;
  // created by user
  protected String createdBy;
  /**
   * The created timestamp in string. The corresponding field in database is of type timestamp.
   * Timestamp auto inserted during insert and update. This field will be primarily used to
   * representing timestamp in string format. The timestamp is retried from database and formatted
   * accordingly in string format
   */
  protected String createdOn;
  // modified by user
  protected String modifiedBy;
  /**
   * The modified timestamp in string. The corresponding field in database is of type timestamp.
   * Timestamp auto inserted during insert and update. This field will be primarily used to
   * representing timestamp in string format. The timestamp is retried from database and formatted
   * accordingly in string format
   */
  protected String modifiedOn;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  public String getModifiedBy() {
    return modifiedBy;
  }

  public void setModifiedBy(String modifiedBy) {
    this.modifiedBy = modifiedBy;
  }

  public String getModifiedOn() {
    return modifiedOn;
  }

  public void setModifiedOn(String modifiedOn) {
    this.modifiedOn = modifiedOn;
  }
}
