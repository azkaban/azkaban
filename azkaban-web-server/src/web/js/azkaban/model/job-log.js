/*
 * Copyright 2014 LinkedIn Corp.
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

$.namespace('azkaban');

azkaban.JobLogModel = Backbone.Model.extend({
  initialize: function() {
    this.set("offset", 0);
    this.set("logData", "");
  },

  refresh: function() {
    var requestURL = contextURL + "/executor";
    var finished = false;

    while (!finished) {
      var requestData = {
        "execid": execId,
        "jobId": jobId,
        "ajax":"fetchExecJobLogs",
        "offset": this.get("offset"),
        "length": 50000,
        "attempt": attempt
      };

      var self = this;

      var successHandler = function(data) {
        console.log("fetchLogs");
        if (data.error) {
          console.log(data.error);
          finished = true;
        }
        else if (data.length == 0) {
          finished = true;
        }
        else {
          self.set("offset", data.offset + data.length);
          self.set("logData", self.get("logData") + data.data);
        }
      }

      $.ajax({
        url: requestURL,
        type: "get",
        async: false,
        data: requestData,
        dataType: "json",
        error: function(data) {
          console.log(data);
          finished = true;
        },
        success: successHandler
      });
    }
  },
});
