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

function removeSched(scheduleId) {
  var scheduleURL = contextURL + "/schedule"
  var redirectURL = contextURL + "/schedule"
  var requestData = {
    "action": "removeSched",
    "scheduleId":scheduleId
  };
  var successHandler = function(data) {
    if (data.error) {
      $('#errorMsg').text(data.error);
    }
    else {
      window.location = redirectURL;
    }
  };
  $.post(scheduleURL, requestData, successHandler, "json");
}

function removeSla(scheduleId) {
  var scheduleURL = contextURL + "/schedule"
  var redirectURL = contextURL + "/schedule"
  var requestData = {
    "action": "removeSla",
    "scheduleId": scheduleId
  };
  var successHandler = function(data) {
    if (data.error) {
      $('#errorMsg').text(data.error)
    }
    else {
      window.location = redirectURL
    }
  };
  $.post(scheduleURL, requestData, successHandler, "json");
}
