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
    $("#timepicker").datetimepicker({pickDate: false});
    $("#datepicker").datetimepicker({pickTime: false});
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
    var timeVal = $('#timepicker').val();
    var timezoneVal = $('#timezone').val();

    var dateVal = $('#datepicker').val();

    var is_recurringVal = $('#is_recurring').val();
    var periodVal = $('#period').val();
    var periodUnits = $('#period_units').val();

    var scheduleURL = contextURL + "/schedule"
    var scheduleData = flowExecuteDialogView.getExecutionOptionData();

    console.log("Creating schedule for " + projectName + "." +
        scheduleData.flow);

    var scheduleTime = moment(timeVal, 'h/mm A').format('h,mm,A,') + timezoneVal;
    console.log(scheduleTime);

    var scheduleDate = $('#datepicker').val();
    var is_recurring = document.getElementById('is_recurring').checked
        ? 'on' : 'off';
    var period = $('#period').val() + $('#period_units').val();

    scheduleData.ajax = "scheduleFlow";
    scheduleData.projectName = projectName;
    scheduleData.scheduleTime = scheduleTime;
    scheduleData.scheduleDate = scheduleDate;
    scheduleData.is_recurring = is_recurring;
    scheduleData.period = period;

    var successHandler = function(data) {
      if (data.error) {
        schedulePanelView.hideSchedulePanel();
        messageDialogView.show("Error Scheduling Flow", data.message);
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
});
