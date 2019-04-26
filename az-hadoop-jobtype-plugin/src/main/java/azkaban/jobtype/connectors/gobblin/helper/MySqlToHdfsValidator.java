/*
 * Copyright 2014-2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobtype.connectors.gobblin.helper;

import azkaban.jobtype.connectors.gobblin.GobblinConstants;
import azkaban.jobtype.javautils.ValidationUtils;
import azkaban.utils.Props;

/**
 * Property validator for preset mySqlToHdfs
 */
public class MySqlToHdfsValidator implements IPropertiesValidator {

  @Override
  public void validate(Props props) {

    ValidationUtils.validateAllNotEmpty(props, GobblinConstants.GOBBLIN_WORK_DIRECTORY_KEY
                                             , "source.querybased.schema" //Database
                                             , "source.entity"            //Table
                                             , "source.conn.host"
                                             , "source.conn.port"
                                             , "source.conn.username"
                                             , "source.conn.password"
                                             , "source.timezone"
                                             , "extract.table.type" //snapshot_only, append_only, snapshot_append
                                             , "data.publisher.final.dir"); //Output directory

    //Validate parameters for watermark
    ValidationUtils.validateAllOrNone(props, "source.querybased.extract.type"
                                           , "extract.delta.fields");
  }
}
