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

var scheduleCustomSvgGraphView;
var scheduleCustomJobListView;

var scheduleFlowView;

var scheduleFlowData;

//function recurseAllAncestors(nodes, disabledMap, id, disable) {
//  var node = nodes[id];
//
//  if (node.inNodes) {
//    for (var key in node.inNodes) {
//      disabledMap[key] = disable;
//      recurseAllAncestors(nodes, disabledMap, key, disable);
//    }
//  }
//}
//
//function recurseAllDescendents(nodes, disabledMap, id, disable) {
//  var node = nodes[id];
//
//  if (node.outNodes) {
//    for (var key in node.outNodes) {
//      disabledMap[key] = disable;
//      recurseAllDescendents(nodes, disabledMap, key, disable);
//    }
//  }
//}
//
azkaban.ScheduleContextMenu = Backbone.View.extend({
  events : {
    "click #scheduleDisableArrow" : "handleDisabledClick",
    "click #scheduleEnableArrow" : "handleEnabledClick"
  },
  initialize: function(settings) {
    $('#scheduleDisableSub').hide();
    $('#scheduleEnableSub').hide();
  },
  handleEnabledClick: function(evt) {
    if(evt.stopPropagation) {
      evt.stopPropagation();
    }
    evt.cancelBubble=true;

    if (evt.currentTarget.expanded) {
      evt.currentTarget.expanded=false;
      $('#scheduleEnableArrow').removeClass('collapse');
      $('#scheduleEnableSub').hide();
    }
    else {
      evt.currentTarget.expanded=true;
      $('#scheduleEnableArrow').addClass('collapse');
      $('#scheduleEnableSub').show();
    }
  },
  handleDisabledClick: function(evt) {
    if(evt.stopPropagation) {
      evt.stopPropagation();
    }
    evt.cancelBubble=true;

    if (evt.currentTarget.expanded) {
      evt.currentTarget.expanded=false;
      $('#scheduleDisableArrow').removeClass('collapse');
      $('#scheduleDisableSub').hide();
    }
    else {
      evt.currentTarget.expanded=true;
      $('#scheduleDisableArrow').addClass('collapse');
      $('#scheduleDisableSub').show();
    }
  }
});

azkaban.ScheduleFlowView = Backbone.View.extend({
  events : {
  "click #schedule-btn": "handleScheduleFlow",
  "click #adv-schedule-opt-btn": "handleAdvancedSchedule"
  },
  initialize : function(settings) {
    $( "#datepicker" ).datepicker();
    $( "#datepicker" ).datepicker('setDate', new Date());
    $("#errorMsg").hide();
  },
  handleAdvancedSchedule : function(evt) {
    console.log("Clicked advanced schedule options button");
    //$('#confirm-container').hide();
    $.modal.close();
    advancedScheduleView.show();
  },
  handleScheduleFlow : function(evt) {

  var hourVal = $('#hour').val();
  var minutesVal = $('#minutes').val();
  var ampmVal = $('#am_pm').val();
  var timezoneVal = $('#timezone').val();
  var dateVal = $('#datepicker').val();
  var is_recurringVal = $('#is_recurring').val();
  var periodVal = $('#period').val();
  var periodUnits = $('#period_units').val();

  console.log("Creating schedule for "+projectName+"."+flowName);
  $.ajax({
    async: "false",
    url: "schedule",
    dataType: "json",
    type: "POST",
    data: {
      action:"scheduleFlow",
      projectId:projectId,
      projectName:projectName,
      flowName:flowName,
      hour:hourVal,
      minutes:minutesVal,
      am_pm:ampmVal,
      timezone:timezoneVal,
      date:dateVal,
      userExec:"dummy",
      is_recurring:is_recurringVal,
      period:periodVal,
      period_units:periodUnits
      },
    success: function(data) {
      if (data.status == "success") {
        console.log("Successfully scheduled for "+projectName+"."+flowName);
        if (data.action == "redirect") {
          window.location = contextURL + "/manager?project=" + projectName + "&flow=" + flowName ;
        }
        else{
          $("#success_message").text("Flow " + projectName + "." + flowName + " scheduled!" );
           window.location = contextURL + "/manager?project=" + projectName + "&flow=" + flowName ;
        }
      }
      else {
        if (data.action == "login") {
          window.location = "";
        }
        else {
          $("#errorMsg").text("ERROR: " + data.message);
          $("#errorMsg").slideDown("fast");
        }
      }
    }
  });

  },
  render: function() {
  }
});

azkaban.AdvancedScheduleView = Backbone.View.extend({
  events : {
    "click" : "closeEditingTarget",
    "click #adv-schedule-btn": "handleAdvSchedule",
    "click #schedule-cancel-btn": "handleCancel",
    "click .modal-close": "handleCancel",
    "click #scheduleGeneralOptions": "handleGeneralOptionsSelect",
    "click #scheduleFlowOptions": "handleFlowOptionsSelect",
    "click #scheduleAddRow": "handleAddRow",
    "click table .editable": "handleEditColumn",
    "click table .removeIcon": "handleRemoveColumn"
  },
  initialize: function(setting) {
    this.contextMenu = new azkaban.ScheduleContextMenu({el:$('#scheduleDisableJobMenu')});
    this.handleGeneralOptionsSelect();
    $( "#advdatepicker" ).datepicker();
    $( "#advdatepicker" ).datepicker('setDate', new Date());
  },
  show: function() {
    $('#scheduleModalBackground').show();
    $('#schedule-options').show();
    this.handleGeneralOptionsSelect();

    scheduleFlowData = this.model.clone();
    this.flowData = scheduleFlowData;
    var flowData = scheduleFlowData;

    var fetchData = {"project": projectName, "ajax":"flowInfo", "flow":flowName};

    var executeURL = contextURL + "/executor";
    this.executeURL = executeURL;
    var scheduleURL = contextURL + "/schedule";
    this.scheduleURL = scheduleURL;
    var handleAddRow = this.handleAddRow;

    var data = flowData.get("data");
    var nodes = {};
    for (var i=0; i < data.nodes.length; ++i) {
      var node = data.nodes[i];
      nodes[node.id] = node;
    }

    for (var i=0; i < data.edges.length; ++i) {
      var edge = data.edges[i];
      var fromNode = nodes[edge.from];
      var toNode = nodes[edge.target];

      if (!fromNode.outNodes) {
        fromNode.outNodes = {};
      }
      fromNode.outNodes[toNode.id] = toNode;

      if (!toNode.inNodes) {
        toNode.inNodes = {};
      }
      toNode.inNodes[fromNode.id] = fromNode;
    }
    flowData.set({nodes: nodes});

    var disabled = {};
    for (var i = 0; i < data.nodes.length; ++i) {
      var updateNode = data.nodes[i];
      if (updateNode.status == "DISABLED" || updateNode.status == "SKIPPED") {
        updateNode.status = "READY";
        disabled[updateNode.id] = true;
      }
      if (updateNode.status == "SUCCEEDED" || updateNode.status=="RUNNING") {
        disabled[updateNode.id] = true;
      }
    }
    flowData.set({disabled: disabled});

    $.get(
      executeURL,
      fetchData,
      function(data) {
        if (data.error) {
          alert(data.error);
        }
        else {
          if (data.successEmails) {
            $('#scheduleSuccessEmails').val(data.successEmails.join());
          }
          if (data.failureEmails) {
            $('#scheduleFailureEmails').val(data.failureEmails.join());
          }

          if (data.failureAction) {
            $('#scheduleFailureAction').val(data.failureAction);
          }
          if (data.notifyFailureFirst) {
            $('#scheduleNotifyFailureFirst').attr('checked', true);
          }
          if (data.notifyFailureLast) {
            $('#scheduleNotifyFailureLast').attr('checked', true);
          }
          if (data.flowParam) {
            var flowParam = data.flowParam;
            for (var key in flowParam) {
              var row = handleAddRow();
              var td = $(row).find('td');
              $(td[0]).text(key);
              $(td[1]).text(flowParam[key]);
            }
          }

          if (!data.running || data.running.length == 0) {
            $(".radio").attr("disabled", "disabled");
            $(".radioLabel").addClass("disabled", "disabled");
          }
        }
      },
      "json"
    );
  },
  handleCancel: function(evt) {
    $('#scheduleModalBackground').hide();
    $('#schedule-options').hide();
  },
  handleGeneralOptionsSelect: function(evt) {
    $('#scheduleFlowOptions').removeClass('selected');
    $('#scheduleGeneralOptions').addClass('selected');

    $('#scheduleGeneralPanel').show();
    $('#scheduleGraphPanel').hide();
  },
  handleFlowOptionsSelect: function(evt) {
    $('#scheduleGeneralOptions').removeClass('selected');
    $('#scheduleFlowOptions').addClass('selected');

    $('#scheduleGraphPanel').show();
    $('#scheduleGeneralPanel').hide();

    if (this.flowSetup) {
      return;
    }

    scheduleCustomSvgGraphView = new azkaban.SvgGraphView({el:$('#scheduleSvgDivCustom'), model: scheduleFlowData, rightClick: {id: 'scheduleDisableJobMenu', callback: this.handleDisableMenuClick}});
    scheduleCustomJobsListView = new azkaban.JobListView({el:$('#scheduleJobListCustom'), model: scheduleFlowData, rightClick: {id: 'scheduleDisableJobMenu', callback: this.handleDisableMenuClick}});
    scheduleFlowData.trigger("change:graph");

    this.flowSetup = true;
  },
  handleAdvSchedule: function(evt) {
    var scheduleURL = this.scheduleURL;
    var disabled = this.flowData.get("disabled");
    var disabledJobs = "";
    for(var job in disabled) {
      if(disabled[job] == true) {
        disabledJobs += "," + job;
      }
    }
    var failureAction = $('#scheduleFailureAction').val();
    var failureEmails = $('#scheduleFailureEmails').val();
    var successEmails = $('#scheduleSuccessEmails').val();
    var notifyFailureFirst = $('#scheduleNotifyFailureFirst').is(':checked');
    var notifyFailureLast = $('#scheduleNotifyFailureLast').is(':checked');
    var executingJobOption = $('input:radio[name=gender]:checked').val();


    var scheduleTime = $('#advhour').val() + "," + $('#advminutes').val() + "," + $('#advam_pm').val() + "," + $('#advtimezone').val();
    var scheduleDate = $('#advdatepicker').val();
    var is_recurring = $('#advis_recurring').val();
    var period = $('#advperiod').val() + $('#advperiod_units').val();

    var flowOverride = {};
    var editRows = $(".editRow");
    for (var i = 0; i < editRows.length; ++i) {
      var row = editRows[i];
      var td = $(row).find('td');
      var key = $(td[0]).text();
      var val = $(td[1]).text();

      if (key && key.length > 0) {
        flowOverride[key] = val;
      }
    }

    var scheduleData = {
      projectId:projectId,
      projectName: projectName,
      ajax: "advSchedule",
      flowName: flowName,
      scheduleTime: scheduleTime,
      scheduleDate: scheduleDate,
      is_recurring: is_recurring,
      period: period,
      disabledJobs: disabledJobs,
      failureAction: failureAction,
      failureEmails: failureEmails,
      successEmails: successEmails,
      notifyFailureFirst: notifyFailureFirst,
      notifyFailureLast: notifyFailureLast,
      executingJobOption: executingJobOption,
      flowOverride: flowOverride
    };

    $.post(
        scheduleURL,
        scheduleData,
        function(data) {
          if (data.error) {
            alert(data.error);
          }
          else {
            window.location = scheduleURL;
          }
        },
        "json"
    )
  },
  handleAddRow: function(evt) {
    var tr = document.createElement("tr");
    var tdName = document.createElement("td");
    var tdValue = document.createElement("td");

    var icon = document.createElement("span");
    $(icon).addClass("removeIcon");
    var nameData = document.createElement("span");
    $(nameData).addClass("spanValue");
    var valueData = document.createElement("span");
    $(valueData).addClass("spanValue");

    $(tdName).append(icon);
    $(tdName).append(nameData);
    $(tdName).addClass("name");
    $(tdName).addClass("editable");

    $(tdValue).append(valueData);
    $(tdValue).addClass("editable");

    $(tr).addClass("editRow");
    $(tr).append(tdName);
    $(tr).append(tdValue);

    $(tr).insertBefore("#scheduleAddRow");
    return tr;
  },
  handleEditColumn : function(evt) {
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
  handleRemoveColumn : function(evt) {
    var curTarget = evt.currentTarget;
    // Should be the table
    var row = curTarget.parentElement.parentElement;
    $(row).remove();
  },
  closeEditingTarget: function(evt) {
    if (this.editingTarget != null && this.editingTarget != evt.target && this.editingTarget != evt.target.parentElement ) {
      var input = $(this.editingTarget).children("input")[0];
      var text = $(input).val();
      $(input).remove();

      var valueData = document.createElement("span");
      $(valueData).addClass("spanValue");
      $(valueData).text(text);

      if ($(this.editingTarget).hasClass("name")) {
        var icon = document.createElement("span");
        $(icon).addClass("removeIcon");
        $(this.editingTarget).append(icon);
      }

      $(this.editingTarget).removeClass("editing");
      $(this.editingTarget).append(valueData);
      this.editingTarget = null;
    }
  },
  handleDisableMenuClick : function(action, el, pos) {
    var flowData = scheduleFlowData;
    var jobid = el[0].jobid;
    var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowName + "&job=" + jobid;
    if (action == "open") {
      window.location.href = requestURL;
    }
    else if(action == "openwindow") {
      window.open(requestURL);
    }
    else if(action == "disable") {
      var disabled = flowData.get("disabled");

      disabled[jobid] = true;
      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if(action == "disableAll") {
      var disabled = flowData.get("disabled");

      var nodes = flowData.get("nodes");
      for (var key in nodes) {
        disabled[key] = true;
      }

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "disableParents") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");
      var inNodes = nodes[jobid].inNodes;

      if (inNodes) {
        for (var key in inNodes) {
          disabled[key] = true;
        }
      }

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "disableChildren") {
      var disabledMap = flowData.get("disabled");
      var nodes = flowData.get("nodes");
      var outNodes = nodes[jobid].outNodes;

      if (outNodes) {
        for (var key in outNodes) {
          disabledMap[key] = true;
        }
      }

      flowData.set({disabled: disabledMap});
      flowData.trigger("change:disabled");
    }
    else if (action == "disableAncestors") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");

      recurseAllAncestors(nodes, disabled, jobid, true);

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "disableDescendents") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");

      recurseAllDescendents(nodes, disabled, jobid, true);

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if(action == "enable") {
      var disabled = flowData.get("disabled");

      disabled[jobid] = false;
      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if(action == "enableAll") {
      disabled = {};
      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "enableParents") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");
      var inNodes = nodes[jobid].inNodes;

      if (inNodes) {
        for (var key in inNodes) {
          disabled[key] = false;
        }
      }

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "enableChildren") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");
      var outNodes = nodes[jobid].outNodes;

      if (outNodes) {
        for (var key in outNodes) {
          disabled[key] = false;
        }
      }

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "enableAncestors") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");

      recurseAllAncestors(nodes, disabled, jobid, false);

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
    else if (action == "enableDescendents") {
      var disabled = flowData.get("disabled");
      var nodes = flowData.get("nodes");

      recurseAllDescendents(nodes, disabled, jobid, false);

      flowData.set({disabled: disabled});
      flowData.trigger("change:disabled");
    }
  }
});
