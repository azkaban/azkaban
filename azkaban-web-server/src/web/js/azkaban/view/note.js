/*
 * Copyright 2017 LinkedIn Corp.
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
$(function () {

  $("#submit-button").click(function () {
    console.log("======create note=====")
    var radioValue = $("input[name='note-type']:checked").val();
    var message = $('#message').val();
    var url = $('#url').val();

    var triggerURL = "/notes";
    var redirectURL = "/notes";
    var requestData = {
      "ajax": "addNote",
      "type": radioValue,
      "message": message,
      "url": url
    };
    var successHandler = function (data) {
      if (data.error) {
        $('#errorMsg').text(data.error);
      }
      else {
        window.location = redirectURL;
      }
    };
    $.post(triggerURL, requestData, successHandler, "json");
  });

  $("#clear-button").click(function () {

    console.log("======form clear=====")
    var requestData = {"ajax": "removeNote"};
    var triggerURL = "/notes";
    var redirectURL = "/notes";
    var successHandler = function (data) {
      if (data.error) {
        $('#errorMsg').text(data.error);
      }
      else {
        window.location = redirectURL;
      }
    };
    $.post(triggerURL, requestData, successHandler, "json");
  });
});
