/*
 * Copyright 2018 LinkedIn Corp.
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

/*
 * List of executing triggers on executing flow page.
 */

var flowTriggerInstanceListView;

function killTrigger(id) {
  var requestData = {"id": id, "ajax": "killRunningTrigger"};
  var successHandler = function (data) {
    console.log("cancel clicked");
    if (data.error) {
      showDialog("Error", data.error);
    }
    else {
      showDialog("Killed", "Trigger " + id + " has been killed.");

    }
  };
  ajaxCall(contextURL + "/flowtriggerinstance", requestData, successHandler);
};

azkaban.FlowTriggerInstanceListView = Backbone.View.extend({
  initialize: function (settings) {
    this.model.bind("change:trigger", this.renderJobs, this);
    this.model.bind("change:update", this.updateJobs, this);
  },

  renderJobs: function (evt) {
    var data = this.model.get("data");
    var lastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
    var executingBody = $("#triggerExecutableBody");
    this.updateJobRow(data.items, executingBody);

    var triggerBody = $("#triggerBody");
    if (data.triggerId) {
      this.updateTriggerRow(data, triggerBody);
    }

  },

  updateJobs: function (evt) {
    var update = this.model.get("update");
    var lastTime = update.endTime == -1
        ? (new Date()).getTime()
        : update.endTime;
    var executingBody = $("#triggerExecutableBody");

    if (update.nodes) {
      this.updateJobRow(update.nodes, executingBody);
    }

    var data = this.model.get("data");
    var flowLastTime = data.endTime == -1
        ? (new Date()).getTime()
        : data.endTime;
    var flowStartTime = data.startTime;
    this.updateProgressBar(data, flowStartTime, flowLastTime);
  },

  updateTriggerRow: function (data, body) {
    if (!data) {
      return;
    }
    this.addTriggerRow(data, body);
  },

  addTriggerRow: function (data, body) {
    var self = this;
    var tr = document.createElement("tr");
    var tdId = document.createElement("td");
    var tdSubmitter = document.createElement("td");
    var tdStart = document.createElement("td");
    var tdEnd = document.createElement("td");
    var tdElapse = document.createElement("td");
    var tdProps = document.createElement("td");
    var buttonProps = document.createElement("BUTTON");
    var tdStatus = document.createElement("td");

    $(tdProps).append(buttonProps);
    buttonProps.setAttribute("class", "btn btn-sm btn-info");
    buttonProps.setAttribute("data-toggle", "modal");
    buttonProps.setAttribute("data-target", "#dependencyList");
    buttonProps.innerHTML = "Show";

    $(tr).append(tdId);
    $(tr).append(tdSubmitter);
    $(tr).append(tdStart);
    $(tr).append(tdEnd);
    $(tr).append(tdElapse);
    $(tr).append(tdStatus);
    $(tr).append(tdProps);

    $(tr).addClass("triggerRow");
    $(tdId).addClass("triggerInstanceId");
    $(tdSubmitter).addClass("triggerSubmitter");
    $(tdStart).addClass("startTime");
    $(tdEnd).addClass("endTime");
    $(tdElapse).addClass("elapsedTime");
    $(tdStatus).addClass("status");
    $(tdProps).addClass("props");

    $(tdId).text(data.triggerId);
    $(tdSubmitter).text(data.triggerSubmitter);

    var startTime = data.triggerStartTime == 0 ? (new Date()).getTime()
        : data.triggerStartTime;

    $(tdStart).text(getDateFormat(new Date(startTime)));

    if (data.triggerEndTime <= 0) {
      $(tdEnd).text("-");
    }
    else {
      $(tdEnd).text(getDateFormat(new Date(data.triggerEndTime)));
    }

    if (data.triggerEndTime <= 0) {
      $(tdElapse).text(
          getDuration(data.triggerStartTime, (new Date()).getTime()));
    }
    else {
      $(tdElapse).text(
          getDuration(data.triggerStartTime, data.triggerEndTime));
    }
    var status = document.createElement("div");
    $(status).addClass("status");
    $(status).addClass(data.triggerStatus);
    $(status).text(data.triggerStatus);
    tdStatus.appendChild(status);

    $("#dependencyList").children("div").children("div").children(
        "div")[1].innerHTML = "<pre>" + data.triggerProps + "</pre>";

    // handle action part
    if (data.triggerStatus === "RUNNING") {
      var tdAction = document.createElement("td");
      var tdActionButton = document.createElement("BUTTON");

      tdActionButton.setAttribute("class", "btn btn-danger btn-sm");
      tdActionButton.setAttribute("onclick", "killTrigger(\"" + data.triggerId
          + "\")");
      tdActionButton.innerHTML = "Kill";
      $(tdAction).append(tdActionButton);
      $(tr).append(tdAction);
    }
    else {
      var tdAction = document.createElement("td");
      $(tdAction).text("-");
      $(tdAction).addClass("triggerAction");
      $(tr).append(tdAction);
    }

    $(body).append(tr);
  },

  updateJobRow: function (nodes, body) {
    if (!nodes) {
      return;
    }

    nodes.sort(function (a, b) {
      return a.dependencyStartTime - b.dependencyStartTime;
    });
    for (var i = 0; i < nodes.length; ++i) {
      this.addNodeRow(nodes[i], body);
    }
  },

  addNodeRow: function (node, body) {
    var self = this;
    var tr = document.createElement("tr");
    var tdId = document.createElement("td");
    var tdName = document.createElement("td");
    var tdType = document.createElement("td");
    //var tdTimeline = document.createElement("td");
    var tdStart = document.createElement("td");
    var tdEnd = document.createElement("td");
    var tdElapse = document.createElement("td");
    var tdStatus = document.createElement("td");
    var tdCancelCause = document.createElement("td");
    //node.joblistrow = tr;
    //tr.node = node;
    var padding = 15 * $(body)[0].level;

    $(tr).append(tdId);
    $(tr).append(tdName);
    $(tr).append(tdType);
    $(tr).append(tdStart);
    $(tr).append(tdEnd);
    $(tr).append(tdElapse);
    $(tr).append(tdStatus);
    $(tr).append(tdCancelCause);
    $(tr).addClass("depListRow");

    $(tdName).addClass("depname");
    $(tdType).addClass("deptype");
    if (padding) {
      $(tdName).css("padding-left", padding);
    }
    $(tdStart).addClass("startTime");
    $(tdEnd).addClass("endTime");
    $(tdElapse).addClass("elapsedTime");
    $(tdStatus).addClass("statustd");

    $(tdId).text(node.triggerInstanceId);
    $(tdName).text(node.dependencyName);
    $(tdType).text(node.dependencyType);

    var status = document.createElement("div");
    $(status).addClass("status");
    $(status).addClass(node.dependencyStatus);
    $(status).text(node.dependencyStatus);
    tdStatus.appendChild(status);

    $(tdCancelCause).text(node.dependencyCancelCause);
    var startTime = node.dependencyStartTime == 0 ? (new Date()).getTime()
        : node.dependencyStartTime;

    var endTime = node.dependencyEndTime;

    $(tdStart).text(getDateFormat(new Date(startTime)));
    if (node.dependencyEndTime <= 0) {
      $(tdEnd).text("-");
    }
    else {
      $(tdEnd).text(getDateFormat(new Date(endTime)));
    }

    if (node.dependencyEndTime <= 0) {
      $(tdElapse).text(
          getDuration(node.dependencyStartTime, (new Date()).getTime()));
    }
    else {
      $(tdElapse).text(
          getDuration(node.dependencyStartTime, node.dependencyEndTime));
    }

    $(body).append(tr);
  }
});

