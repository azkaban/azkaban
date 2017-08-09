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
$.namespace('azkaban');

$(function() {

  $('.show-and-hide-content').click(function () {
    if ($('input.select-yes:checked').prop('checked')) {
      $('.show-and-hide-true').show('slideToggle');
    }
    else{
      $('.show-and-hide-true').hide('slideToggle');
    }
  });


  $("#buttonid").click(function(){
    var radioValue = $("input[name='foo']:checked").val();
    var message = $('#message').val();
    var url = $('#url').val();

    if(radioValue == "Countdown")
      message =  $('#firstName').val();

    var type = radioValue;
    var triggerURL = contextURL + "/notes";
    var redirectURL = contextURL + "/notes";
    var requestData = {"ajax": "addNote", "type": type, "message": message, "url": url};
    var successHandler = function(data) {
      if (data.error) {
        alert(data.error);
        $('#errorMsg').text(data.error);
      }
      else {
        window.location = redirectURL;
      }
    };
    console.log("===== triggerURL :" + triggerURL);
    console.log("===== requestData: " + requestData);

    $.post(triggerURL, requestData, successHandler, "json");

  });

  $("#buttonid2").click(function(){

    console.log("form clear")
    var requestData = {"ajax": "removeNote"};
    var triggerURL = contextURL + "/notes";
    var redirectURL = contextURL + "/notes";
    var successHandler = function(data) {
      if (data.error) {
        //alert(data.error)
        $('#errorMsg').text(data.error);
      }
      else {
        window.location = redirectURL;
      }
    };
    $.post(triggerURL, requestData, successHandler, "json");

  });
  console.log("===== before adsfasdfasdf =======");
});
