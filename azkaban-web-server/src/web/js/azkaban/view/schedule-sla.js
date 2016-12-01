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

azkaban.ChangeSlaView = Backbone.View.extend({
  events: {
    "click": "closeEditingTarget",
    "click #set-sla-btn": "handleSetSla",
    "click #remove-sla-btn": "handleRemoveSla",
    "click #add-btn": "handleAddRow"
  },

  initialize: function(setting) {
    $('#sla-options').on('hidden.bs.modal', function() {
      slaView.handleSlaCancel();
    });
  },

  handleSlaCancel: function() {
    console.log("Clicked cancel button");
    var scheduleURL = contextURL + "/schedule";
    var tFlowRules = document.getElementById("flowRulesTbl").tBodies[0];
    var rows = tFlowRules.rows;
    var rowLength = rows.length
    for (var i = 0; i < rowLength-1; i++) {
      tFlowRules.deleteRow(0);
    }
  },

  initFromSched: function(scheduleId, flowName) {
    this.scheduleId = scheduleId;
    var scheduleURL = contextURL + "/schedule"
    this.scheduleURL = scheduleURL;

    var indexToName = {};
    var nameToIndex = {};
    var indexToText = {};
    this.indexToName = indexToName;
    this.nameToIndex = nameToIndex;
    this.indexToText = indexToText;

    var ruleBoxOptions = ["SUCCESS", "FINISH"];
    this.ruleBoxOptions = ruleBoxOptions;

    var fetchScheduleData = {
      "scheduleId": this.scheduleId,
      "ajax": "slaInfo"
    };

    var successHandler = function(data) {
      if (data.error) {
        alert(data.error);
        return;
      }
      if (data.slaEmails) {
        $('#slaEmails').val(data.slaEmails.join());
      }

      var allJobNames = data.allJobNames;

      indexToName[0] = "";
      nameToIndex[flowName] = 0;
      indexToText[0] = "flow " + flowName;
      for (var i = 1; i <= allJobNames.length; i++) {
        indexToName[i] = allJobNames[i-1];
        nameToIndex[allJobNames[i-1]] = i;
        indexToText[i] = "job " + allJobNames[i-1];
      }

      // populate with existing settings
      if (data.settings) {
        var tFlowRules = document.getElementById("flowRulesTbl").tBodies[0];
        for (var setting in data.settings) {
          var rFlowRule = tFlowRules.insertRow(0);

          var cId = rFlowRule.insertCell(-1);
          var idSelect = document.createElement("select");
          idSelect.setAttribute("class", "form-control");
          for (var i in indexToName) {
            idSelect.options[i] = new Option(indexToText[i], indexToName[i]);
            if (data.settings[setting].id == indexToName[i]) {
              idSelect.options[i].selected = true;
            }
          }
          cId.appendChild(idSelect);

          var cRule = rFlowRule.insertCell(-1);
          var ruleSelect = document.createElement("select");
          ruleSelect.setAttribute("class", "form-control");
          for (var i in ruleBoxOptions) {
            ruleSelect.options[i] = new Option(ruleBoxOptions[i], ruleBoxOptions[i]);
            if (data.settings[setting].rule == ruleBoxOptions[i]) {
              ruleSelect.options[i].selected = true;
            }
          }
          cRule.appendChild(ruleSelect);

          var cDuration = rFlowRule.insertCell(-1);
          var duration = document.createElement("input");
          duration.type = "text";
          duration.setAttribute("class", "form-control durationpick");
          var rawMinutes = data.settings[setting].duration;
          var intMinutes = rawMinutes.substring(0, rawMinutes.length-1);
          var minutes = parseInt(intMinutes);
          var hours = Math.floor(minutes / 60);
          minutes = minutes % 60;
          duration.value = hours + ":" + minutes;
          cDuration.appendChild(duration);

          var cEmail = rFlowRule.insertCell(-1);
          var emailCheck = document.createElement("input");
          emailCheck.type = "checkbox";
          for (var act in data.settings[setting].actions) {
            if (data.settings[setting].actions[act] == "EMAIL") {
              emailCheck.checked = true;
            }
          }
          cEmail.appendChild(emailCheck);

          var cKill = rFlowRule.insertCell(-1);
          var killCheck = document.createElement("input");
          killCheck.type = "checkbox";
          for (var act in data.settings[setting].actions) {
            if (data.settings[setting].actions[act] == "KILL") {
              killCheck.checked = true;
            }
          }
          cKill.appendChild(killCheck);
          $('.durationpick').datetimepicker({
            format: 'HH:mm'
          });
        }
      }
      $('.durationpick').datetimepicker({
        format: 'HH:mm'
      });
    };

    $.get(this.scheduleURL, fetchScheduleData, successHandler, "json");

    $('#sla-options').modal();

    //this.schedFlowOptions = sched.flowOptions
    console.log("Loaded schedule info. Ready to set SLA.");
  },

  handleRemoveSla: function(evt) {
    console.log("Clicked remove sla button");
    var scheduleURL = this.scheduleURL;
    var redirectURL = this.scheduleURL;
    var requestData = {
      "action": "removeSla",
      "scheduleId": this.scheduleId
    };
    var successHandler = function(data) {
      if (data.error) {
        $('#errorMsg').text(data.error)
      }
      else {
        window.location = redirectURL
      }
    };
    $.post(scheduleURL, requestData, successHanlder, "json");
  },

  handleSetSla: function(evt) {
    var slaEmails = $('#slaEmails').val();
    var settings = {};
    var tFlowRules = document.getElementById("flowRulesTbl").tBodies[0];
    for (var row = 0; row < tFlowRules.rows.length-1; row++) {
      var rFlowRule = tFlowRules.rows[row];
      var id = rFlowRule.cells[0].firstChild.value;
      var rule = rFlowRule.cells[1].firstChild.value;
      var duration = rFlowRule.cells[2].firstChild.value;
      var email = rFlowRule.cells[3].firstChild.checked;
      var kill = rFlowRule.cells[4].firstChild.checked;
      settings[row] = id + "," + rule + "," + duration + "," + email + "," + kill;
    }

    var slaData = {
      scheduleId: this.scheduleId,
      ajax: "setSla",
      slaEmails: slaEmails,
      settings: settings
    };

    var scheduleURL = this.scheduleURL;
    var successHandler = function(data) {
      if (data.error) {
        alert(data.error);
      }
      else {
        tFlowRules.length = 0;
        window.location = scheduleURL;
      }
    };
    $.post(scheduleURL, slaData, successHandler, "json");
  },

  handleAddRow: function(evt) {
    var indexToName = this.indexToName;
    var nameToIndex = this.nameToIndex;
    var indexToText = this.indexToText;
    var ruleBoxOptions = this.ruleBoxOptions;

    var tFlowRules = document.getElementById("flowRulesTbl").tBodies[0];
    var rFlowRule = tFlowRules.insertRow(tFlowRules.rows.length-1);

    var cId = rFlowRule.insertCell(-1);
    var idSelect = document.createElement("select");
    idSelect.setAttribute("class", "form-control");
    for (var i in indexToName) {
      idSelect.options[i] = new Option(indexToText[i], indexToName[i]);
    }
    cId.appendChild(idSelect);

    var cRule = rFlowRule.insertCell(-1);
    var ruleSelect = document.createElement("select");
    ruleSelect.setAttribute("class", "form-control");
    for (var i in ruleBoxOptions) {
      ruleSelect.options[i] = new Option(ruleBoxOptions[i], ruleBoxOptions[i]);
    }
    cRule.appendChild(ruleSelect);

    var cDuration = rFlowRule.insertCell(-1);
    var duration = document.createElement("input");
    duration.type = "text";
    duration.setAttribute("class", "durationpick form-control");
    cDuration.appendChild(duration);

    var cEmail = rFlowRule.insertCell(-1);
    var emailCheck = document.createElement("input");
    emailCheck.type = "checkbox";
    cEmail.appendChild(emailCheck);

    var cKill = rFlowRule.insertCell(-1);
    var killCheck = document.createElement("input");
    killCheck.type = "checkbox";
    cKill.appendChild(killCheck);

    $('.durationpick').datetimepicker({
      format: 'HH:mm'
    });
    return rFlowRule;
  },

  handleEditColumn: function(evt) {
    var curTarget = evt.currentTarget;
    if (this.editingTarget != curTarget) {
      this.closeEditingTarget();

      var text = $(curTarget).children(".spanValue").text();
      $(curTarget).empty();

      var input = document.createElement("input");
      $(input).attr("type", "text");
      $(input).css("width", "100%");
      $(input).val(text);
      $(curTarget).addClass("editing");
      $(curTarget).append(input);
      $(input).focus();
      this.editingTarget = curTarget;
    }
  },

  handleRemoveColumn: function(evt) {
    var curTarget = evt.currentTarget;
    // Should be the table
    var row = curTarget.parentElement.parentElement;
    $(row).remove();
  },

  closeEditingTarget: function(evt) {
  }
});
