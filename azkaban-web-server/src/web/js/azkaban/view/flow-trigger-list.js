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

/*
 * List of executing jobs on executing flow page.
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
      showDialog("Killed", "Trigger has been killed.");

    }
  };
  ajaxCall(contextURL + "/flowtriggerinstance", requestData, successHandler);
};

azkaban.FlowTriggerInstanceListView = Backbone.View.extend({
  events: {
    //"contextmenu.flow-progress-bar": "handleProgressBoxClick"
  },

  initialize: function (settings) {
    this.model.bind("change:trigger", this.renderJobs, this);
    this.model.bind("change:update", this.updateJobs, this);
    // This is for tabbing. Blah, hacky
    //var executingBody = $("#triggerExecutableBody")[0];
    //executingBody.level = 0;
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
    var tdStatus = document.createElement("td");
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

    //alert(data.triggerId);
    //alert(tr);
    //$(tr).text(data.triggerId);
    $(tdId).text(data.triggerId);
    $(tdSubmitter).text(data.triggerSubmitter);

    var startTime = data.triggerStartTime == 0 ? (new Date()).getTime()
        : data.triggerStartTime;

    var endTime = data.triggerEndTime == 0 ? (new Date()).getTime()
        : data.triggerEndTime;

    $(tdStart).text(getDateFormat(new Date(startTime)));
    $(tdEnd).text(getDateFormat(new Date(endTime)));

    if (data.triggerEndTime == 0) {
      $(tdElapse).text(
          getDuration(data.triggerStartTime, (new Date()).getTime()));
    }
    else {
      $(tdElapse).text(
          getDuration(data.triggerStartTime, data.triggerEndTime));
    }
    $(tdStatus).text(data.triggerStatus);

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

    /*
    $(tdSubmitter).addClass("triggerSubmitter");
    $(tdStart).addClass("startTime");
    $(tdEnd).addClass("endTime");
    $(tdElapse).addClass("elapsedTime");
    $(tdStatus).addClass("status");
    $(tdProps).addClass("props");*/
  }
  ,

  updateJobRow: function (nodes, body) {
    if (!nodes) {
      return;
    }

    nodes.sort(function (a, b) {
      return a.dependencyStartTime - b.dependencyStartTime;
    });
    for (var i = 0; i < nodes.length; ++i) {
      this.addNodeRow(nodes[i], body);

      // var node = nodes[i].changedNode ? nodes[i].changedNode : nodes[i];
      //
      // if (node.status == 'READY') {
      //   continue;
      // }
      //
      // //var nodeId = node.id.replace(".", "\\\\.");
      // var row = node.joblistrow;
      // if (!row) {
      //   this.addNodeRow(node, body);
      // }
      //
      // row = node.joblistrow;
      // var statusDiv = $(row).find("> td.statustd > .status");
      // statusDiv.text(statusStringMap[node.status]);
      // $(statusDiv).attr("class", "status " + node.status);
      //
      // var startTimeTd = $(row).find("> td.startTime");
      // if (node.startTime == -1) {
      //   $(startTimeTd).text("-");
      // }
      // else {
      //   var startdate = new Date(node.startTime);
      //   $(startTimeTd).text(getDateFormat(startdate));
      // }
      //
      // var endTimeTd = $(row).find("> td.endTime");
      // if (node.endTime == -1) {
      //   $(endTimeTd).text("-");
      // }
      // else {
      //   var enddate = new Date(node.endTime);
      //   $(endTimeTd).text(getDateFormat(enddate));
      // }
      //
      // var progressBar = $(row).find(
      //     "> td.timeline > .flow-progress > .main-progress");
      // if (!progressBar.hasClass(node.status)) {
      //   for (var j = 0; j < statusList.length; ++j) {
      //     var status = statusList[j];
      //     progressBar.removeClass(status);
      //   }
      //   progressBar.addClass(node.status);
      // }
      //
      // // Create past attempts
      // if (node.pastAttempts) {
      //   for (var a = 0; a < node.pastAttempts.length; ++a) {
      //     var attempt = node.pastAttempts[a];
      //     var attemptBox = attempt.attemptBox;
      //
      //     if (!attemptBox) {
      //       var attemptBox = document.createElement("div");
      //       attempt.attemptBox = attemptBox;
      //
      //       $(attemptBox).addClass("flow-progress-bar");
      //       $(attemptBox).addClass("attempt");
      //
      //       $(attemptBox).css("float", "left");
      //       $(attemptBox).bind("contextmenu", attemptRightClick);
      //
      //       $(progressBar).before(attemptBox);
      //       attemptBox.job = node.nestedId;
      //       attemptBox.attempt = a;
      //     }
      //   }
      // }
      //
      // var elapsedTime = $(row).find("> td.elapsedTime");
      // if (node.endTime == -1) {
      //   $(elapsedTime).text(
      //       getDuration(node.startTime, (new Date()).getTime()));
      // }
      // else {
      //   $(elapsedTime).text(getDuration(node.startTime, node.endTime));
      // }
      //
      // if (node.nodes) {
      //   var subtableBody = $(row.subflowrow).find("> td > table");
      //   subtableBody[0].level = $(body)[0].level + 1;
      //   this.updateJobRow(node.nodes, subtableBody);
      // }
    }
  }
  ,

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
    $(tdStatus).text(node.dependencyStatus);
    $(tdCancelCause).text(node.dependencyCancelCause);
    var startTime = node.dependencyStartTime == 0 ? (new Date()).getTime()
        : node.dependencyStartTime;

    var endTime = node.dependencyEndTime == 0 ? (new Date()).getTime()
        : node.dependencyEndTime;

    $(tdStart).text(getDateFormat(new Date(startTime)));
    $(tdEnd).text(getDateFormat(new Date(endTime)));

    if (node.dependencyEndTime == 0) {
      $(tdElapse).text(
          getDuration(node.dependencyStartTime, (new Date()).getTime()));
    }
    else {
      $(tdElapse).text(
          getDuration(node.dependencyStartTime, node.dependencyEndTime));
    }

    $(body).append(tr);

    /*
    var outerProgressBar = document.createElement("div");
    //$(outerProgressBar).attr("id", node.id + "-outerprogressbar");
    $(outerProgressBar).addClass("flow-progress");

    var progressBox = document.createElement("div");
    progressBox.job = node.id;
    //$(progressBox).attr("id", node.id + "-progressbar");
    $(progressBox).addClass("flow-progress-bar");
    $(progressBox).addClass("main-progress");
    $(outerProgressBar).append(progressBox);
    $(tdTimeline).append(outerProgressBar);

    var requestURL = contextURL + "/manager?project=" + projectName + "&job="
        + node.id + "&history";
    var a = document.createElement("a");
    $(a).attr("href", requestURL);
    $(a).text(node.id);
    $(tdName).append(a);
    if (node.type == "flow") {
      var expandIcon = document.createElement("div");
      $(expandIcon).addClass("listExpand");
      $(tdName).append(expandIcon);
      $(expandIcon).addClass("expandarrow glyphicon glyphicon-chevron-down");
      $(expandIcon).click(function (evt) {
        var parent = $(evt.currentTarget).parents("tr")[0];
        self.toggleExpandFlow(parent.node);
      });
    }

    var status = document.createElement("div");
    $(status).addClass("status");
    //$(status).attr("id", node.id + "-status-div");
    tdStatus.appendChild(status);

    var logURL = contextURL + "/executor?execid=" + execId + "&job="
        + node.nestedId;
    if (node.attempt) {
      logURL += "&attempt=" + node.attempt;
    }

    if (node.type != 'flow' && node.status != 'SKIPPED') {
      var a = document.createElement("a");
      $(a).attr("href", logURL);
      //$(a).attr("id", node.id + "-log-link");
      $(a).text("Details");
      $(tdDetails).append(a);
    }

    $(body).append(tr);
    if (node.type == "flow") {
      var subFlowRow = document.createElement("tr");
      var subFlowCell = document.createElement("td");
      $(subFlowCell).addClass("subflowrow");

      var numColumn = $(tr).children("td").length;
      $(subFlowCell).attr("colspan", numColumn);
      tr.subflowrow = subFlowRow;

      $(subFlowRow).append(subFlowCell);
      $(body).append(subFlowRow);
      $(subFlowRow).hide();
      var subtable = document.createElement("table");
      var parentClasses = $(body).closest("table").attr("class");

      $(subtable).attr("class", parentClasses);
      $(subtable).addClass("subtable");
      $(subFlowCell).append(subtable);
    }*/
  }
});

