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

$.namespace('azkaban');

var schedulePanelView;
azkaban.SchedulePanelView = Backbone.View.extend({
  events: {
    "click #schedule-button": "scheduleFlow"
  },

  initialize: function(settings) {
  },

  render: function() {
  },

  showSchedulePanel: function() {
    $('#schedule-modal').modal();
  },

  hideSchedulePanel: function() {
    $('#schedule-modal').modal("hide");
  },

  scheduleFlow: function() {
    var scheduleURL = contextURL + "/schedule"
    var scheduleData = flowExecuteDialogView.getExecutionOptionData();

    console.log("Creating schedule for " + projectName + "." + scheduleData.flow);

    var currentMomentTime = moment();
    var scheduleTime = currentMomentTime.utc().format('h,mm,A,')+"UTC";
    var scheduleDate = currentMomentTime.format('MM/DD/YYYY');

    scheduleData.ajax = "scheduleCronFlow";
    scheduleData.projectName = projectName;
    scheduleData.cronExpression = "0 " + $('#cron-output').val();

    // Currently, All cron expression will be based on server timezone.
    // Later we might implement a feature support cron under various timezones, depending on the future use cases.
    // scheduleData.cronTimezone = timezone;

    console.log("current Time = " + scheduleDate + "  " + scheduleTime );
    console.log("cronExpression = " +  scheduleData.cronExpression);

    var successHandler = function(data) {
      if (data.error) {
        schedulePanelView.hideSchedulePanel();
        messageDialogView.show("Error Scheduling Flow", data.error);
      }
      else {
        schedulePanelView.hideSchedulePanel();
        messageDialogView.show("Flow Scheduled", data.message, function() {
          window.location.href = scheduleURL;
        });
      }
    };

    $.post(scheduleURL, scheduleData, successHandler, "json");
  }
});

$(function() {
  schedulePanelView =  new azkaban.SchedulePanelView({
    el: $('#schedule-modal')
  });

  // To compute the current timezone's time offset against UTC.
  // Currently not useful.
  // var TimeZoneOffset = new Date().toString().match(/([-\+][0-9]+)\s/)[1];

  $('#timeZoneID').html(timezone);

  updateOutput();
  $("#clearCron").click(function () {
    $('#cron-output').val("* * ? * *");
    resetLabelColor();
    $("#minute_input").val("*");
    $("#hour_input").val("*");
    $("#dom_input").val("?");
    $("#month_input").val("*");
    $("#dow_input").val("*");
    $(cron_translate_id).text("")
    $(cron_translate_warning_id).text("")
    $('#nextRecurId').html("");

    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
  });

  $("#minute_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#min_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("0-59");
    $('#instructions tbody tr:last td').html("allowed values");
  });

  $("#hour_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#hour_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("0-23");
    $('#instructions tbody tr:last td').html("allowed values");
  });

  $("#dom_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#dom_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("1-31");
    $('#instructions tbody tr:last td').html("allowed values");

    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last').find('td').css({'class': 'danger'});
    $('#instructions tbody tr:last th').html("?");
    $('#instructions tbody tr:last td').html("Blank");
  });

  $("#month_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#mon_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("1-12");
    $('#instructions tbody tr:last td').html("allowed values");
  });

  $("#dow_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#dow_label").css("color", "red");

    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("1-7");
    $('#instructions tbody tr:last td').html("SUN MON TUE WED THU FRI SAT");

    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("?");
    $('#instructions tbody tr:last td').html("Blank");
  });
});

function resetLabelColor(){
  $("#min_label").css("color", "black");
  $("#hour_label").css("color", "black");
  $("#dom_label").css("color", "black");
  $("#mon_label").css("color", "black");
  $("#dow_label").css("color", "black");
}

var cron_minutes_id = "#minute_input";
var cron_hours_id   = "#hour_input";
var cron_dom_id     = "#dom_input";
var cron_months_id  = "#month_input";
var cron_dow_id     = "#dow_input";
var cron_output_id  = "#cron-output";
var cron_translate_id  = "#cronTranslate";
var cron_translate_warning_id  = "#translationWarning";

function updateOutput() {
  $(cron_output_id).val( $(cron_minutes_id).val() + " " +  $(cron_hours_id).val() + " " +
      $(cron_dom_id).val() + " " + $(cron_months_id).val() + " " + $(cron_dow_id).val()
  );
  updateExpression();
}

function updateExpression() {
  $('#nextRecurId').html("");

  // transformFromQuartzToUnixCron is defined in util/date.js
  var unixCronStr = transformFromQuartzToUnixCron($(cron_output_id).val());
  console.log("Parsed Unix cron = " + unixCronStr);
  var laterCron = later.parse.cron(unixCronStr);

  //Get the current time given the server timezone.
  var serverTime = moment().tz(timezone);
  console.log("serverTime = " + serverTime.format());
  var now1Str = serverTime.format();

  //Get the server Timezone offset against UTC (e.g. if timezone is PDT, it should be -07:00)
  // var timeZoneOffset = now1Str.substring(now1Str.length-6, now1Str.length);
  // console.log("offset = " + timeZoneOffset);

  //Transform the moment time to UTC Date time (required by later.js)
  var serverTimeInJsDateFormat = new Date();
  serverTimeInJsDateFormat.setUTCHours(serverTime.get('hour'), serverTime.get('minute'), 0, 0);
  serverTimeInJsDateFormat.setUTCMonth(serverTime.get('month'), serverTime.get('date'));

  // Calculate the following 10 occurences based on the current server time.
  // The logic is a bit tricky here. since later.js only support UTC Date (javascript raw library).
  // We transform from current browser-timezone-time to Server timezone.
  // Then we let serverTimeInJsDateFormat is equal to the server time.
  var occurrences = later.schedule(laterCron).next(10, serverTimeInJsDateFormat);

  //The following component below displays a list of next 10 triggering timestamp.
  for(var i = 9; i >= 0; i--) {
    var strTime = JSON.stringify(occurrences[i]);

    // Get the time. The original occurance time string is like: "2016-09-09T05:00:00.999",
    // We trim the string to ignore milliseconds.
    var nextTime = '<li style="color:DarkGreen">' + strTime.substring(1, strTime.length-6) + '</li>';
    $('#nextRecurId').prepend(nextTime);
  }
}
