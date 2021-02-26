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
package azkaban.imagemgmt.dto;

public abstract class BaseDTO {

  // Id of the record
  protected int id;
  // Created by user
  protected String createdBy;
  /**
   * The created timestamp in string. The corresponding field in database is of type timestamp.
   * Timestamp auto inserted during insert and update. This field will be primarily used to
   * representing timestamp in string format. The timestamp is retried from database and formatted
   * accordingly in string format
   */
  protected String createdOn;
  // Modified by user
  protected String modifiedBy;
  /**
   * The modified timestamp in string. The corresponding field in database is of type timestamp.
   * Timestamp auto inserted during insert and update. This field will be primarily used to
   * representing timestamp in string format. The timestamp is retried from database and formatted
   * accordingly in string format
   */
  protected String modifiedOn;

  public int getId() {
    return this.id;
  }

  public void setId(final int id) {
    this.id = id;
  }

  public String getCreatedBy() {
    return this.createdBy;
  }

  public void setCreatedBy(final String createdBy) {
    this.createdBy = createdBy;
  }

  public String getCreatedOn() {
    return this.createdOn;
  }

  public void setCreatedOn(final String createdOn) {
    this.createdOn = createdOn;
  }

  public String getModifiedBy() {
    return this.modifiedBy;
  }

  public void setModifiedBy(final String modifiedBy) {
    this.modifiedBy = modifiedBy;
  }

  public String getModifiedOn() {
    return this.modifiedOn;
  }

  public void setModifiedOn(final String modifiedOn) {
    this.modifiedOn = modifiedOn;
  }

  // Used for grouping all the validations during create
  public interface ValidationOnCreate {

  }

  // Used for grouping all the validations during update
  public interface ValidationOnUpdate {

  }
}
