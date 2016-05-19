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

azkaban.ViewTriggerStatusView = Backbone.View.extend({
  events : {
    "click" : "closeEditingTarget",
  },

  initialize : function(setting) {

  },

  initFromSched : function(scheduleId) {

    this.scheduleId = scheduleId;
    var scheduleURL = contextURL + "/schedule"
    this.scheduleURL = scheduleURL;

    var fetchTriggerData = {
      "scheduleId" : this.scheduleId,
      "ajax" : "fetchNyxTriggerStatus"
    };

    var showDialog = function(title, message) {
      $('#messageTitle').text(title);
      $('#messageBox').text(message);
      $('#messageDialog').modal();
    }

    // main function to handle the data communication.
    // Note : "NyxTriggerChecker_1" is the name of the nyx checker defined in the servlet.
    // we need this value to locate the result in the result bag, if the value changed from the
    // server side we need to make the corresponding changes here as well.
    // returning result.
    var successHandler = function(data) {
      // only process if the expected value is returned in the data bag.
      if (data.NyxTriggerChecker_1 != undefined && data.NyxTriggerChecker_1 != null) {

        var trigger = data.NyxTriggerChecker_1;
        var ruleRel = "unknown";
        var triggerActive = trigger.active ? "<span class='label label-success'>Active</span>"
            : "<span class='label label-danger'>Inactive</span>";
        var triggerReady = trigger.ready ? "<span class='label label-success'>Ready</span>"
            : "<span class='label label-danger'>NotReady</span>";
        var triggerName = trigger.name ? trigger.name : "unknown";
        var triggerOwner = trigger.owner ? trigger.owner : "unknown";
        var triggerCreatedTM = trigger.createdTs ? (new Date(trigger.createdTs)).toString() : "unknown";
        var triggerUpdatedTM = trigger.updatedTs ? (new Date(trigger.updatedTs)).toString() : "unknown";

        if (trigger.ruleSet != undefined && trigger.ruleSet != null) {
          // retrieve the rule relation from the expression field.
          ruleRel = trigger.ruleSet.expression ? trigger.ruleSet.expression : ruleRel;

          // fill in the list for the rules.
          if (trigger.ruleSet.ruleList != undefined && trigger.ruleSet.ruleList != null) {
            // clear the tbody each time the dialog is rendered.
            $('#triggerStatusTbl tbody tr').remove();

            var ruleList = trigger.ruleSet.ruleList;
            var ruleTbl = $('#triggerStatusTbl').get(0).tBodies[0];

            // fill in the rule info. data to render :
            // - name , type , status , Update time
            for ( var idx in ruleList) {
              var row = ruleTbl.insertRow();
              row.insertCell().innerText = ruleList[idx].name;
              row.insertCell().innerText = ruleList[idx].ruleType;
              row.insertCell().innerHTML = ruleList[idx].ready ? "<span class='label label-success'>Ready</span>"
                  : "<span class='label label-danger'>NotReady</span>";
              row.insertCell().innerText = ruleList[idx].updatedTs ? (new Date(ruleList[idx].updatedTs)).toString()
                  : "unknown";
            }
          }
        }

        // fill the place holders on UI.
        $('#triggerName').text(triggerName);
        $('#triggerOwner').text(triggerOwner);
        $('#triggerActive').html(triggerActive);
        $('#triggerReady').html(triggerReady);
        $('#triggerCreatedTM').text(triggerCreatedTM);
        $('#triggerUpdatedTM').text(triggerUpdatedTM);
        $('#ruleRel').text(ruleRel);
        $('#rawData').val(JSON.stringify(trigger, null, 8));

        // modal the UI after all the info is successfully received.
        $('#trigger-status').modal();

        console.log("Loaded trigger info.");
      } else {
        showDialog("Unable to get trigger info", "Unable to get trigger info for the schedule,"
            + " the schduled flow either doesn't have a trigger defined or the trigger service is unavailable."
            + " (NYXTRIGGERCHECKER_1_NOT_FOUND)");
        return;
      }
    };

    $.get(this.scheduleURL, fetchTriggerData, successHandler, "json");
  },

  closeEditingTarget : function() {
  }

});
