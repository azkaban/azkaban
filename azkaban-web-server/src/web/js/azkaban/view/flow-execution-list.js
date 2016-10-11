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

var executionListView;
azkaban.ExecutionListView = Backbone.View.extend({
  events: {
    //"contextmenu .flow-progress-bar": "handleProgressBoxClick"
  },

  initialize: function(settings) {
    this.model.bind('change:graph', this.renderJobs, this);
    this.model.bind('change:update', this.updateJobs, this);

    // This is for tabbing. Blah, hacky
    var executingBody = $("#executableBody")[0];
    executingBody.level = 0;
  },

  renderJobs: function(evt) {
    var data = this.model.get("data");
    var lastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
    var executingBody = $("#executableBody");
    this.updateJobRow(data.nodes, executingBody);

    var flowLastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
    var flowStartTime = data.startTime;
    this.updateProgressBar(data, flowStartTime, flowLastTime);
  },

//
//  handleProgressBoxClick: function(evt) {
//    var target = evt.currentTarget;
//    var job = target.job;
//    var attempt = target.attempt;
//
//    var data = this.model.get("data");
//    var node = data.nodes[job];
//
//    var jobId = event.currentTarget.jobid;
//    var requestURL = contextURL + "/manager?project=" + projectName + "&execid=" + execId + "&job=" + job + "&attempt=" + attempt;
//
//    var menu = [
//        {title: "Open Job...", callback: function() {window.location.href=requestURL;}},
//        {title: "Open Job in New Window...", callback: function() {window.open(requestURL);}}
//    ];
//
//    contextMenuView.show(evt, menu);
//  },

  updateJobs: function(evt) {
    var update = this.model.get("update");
    var lastTime = update.endTime == -1
        ? (new Date()).getTime()
        : update.endTime;
    var executingBody = $("#executableBody");

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

  updateJobRow: function(nodes, body) {
    if (!nodes) {
      return;
    }

    nodes.sort(function(a,b) { return a.startTime - b.startTime; });
    for (var i = 0; i < nodes.length; ++i) {
      var node = nodes[i].changedNode ? nodes[i].changedNode : nodes[i];

      if (node.startTime < 0) {
        continue;
      }
      //var nodeId = node.id.replace(".", "\\\\.");
      var row = node.joblistrow;
      if (!row) {
        this.addNodeRow(node, body);
      }

      row = node.joblistrow;
      var statusDiv = $(row).find("> td.statustd > .status");
      statusDiv.text(statusStringMap[node.status]);
      $(statusDiv).attr("class", "status " + node.status);

      var startTimeTd = $(row).find("> td.startTime");
      var startdate = new Date(node.startTime);
      $(startTimeTd).text(getDateFormat(startdate));

      var endTimeTd = $(row).find("> td.endTime");
      if (node.endTime == -1) {
        $(endTimeTd).text("-");
      }
      else {
        var enddate = new Date(node.endTime);
        $(endTimeTd).text(getDateFormat(enddate));
      }

      var progressBar = $(row).find("> td.timeline > .flow-progress > .main-progress");
      if (!progressBar.hasClass(node.status)) {
        for (var j = 0; j < statusList.length; ++j) {
          var status = statusList[j];
          progressBar.removeClass(status);
        }
        progressBar.addClass(node.status);
      }

      // Create past attempts
      if (node.pastAttempts) {
        for (var a = 0; a < node.pastAttempts.length; ++a) {
          var attempt = node.pastAttempts[a];
          var attemptBox = attempt.attemptBox;

          if (!attemptBox) {
            var attemptBox = document.createElement("div");
            attempt.attemptBox = attemptBox;

            $(attemptBox).addClass("flow-progress-bar");
            $(attemptBox).addClass("attempt");

            $(attemptBox).css("float","left");
            $(attemptBox).bind("contextmenu", attemptRightClick);

            $(progressBar).before(attemptBox);
            attemptBox.job = node.nestedId;
            attemptBox.attempt = a;
          }
        }
      }

      var elapsedTime = $(row).find("> td.elapsedTime");
      if (node.endTime == -1) {
        $(elapsedTime).text(getDuration(node.startTime, (new Date()).getTime()));
      }
      else {
        $(elapsedTime).text(getDuration(node.startTime, node.endTime));
      }

      if (node.nodes) {
        var subtableBody = $(row.subflowrow).find("> td > table");
        subtableBody[0].level = $(body)[0].level + 1;
        this.updateJobRow(node.nodes, subtableBody);
      }
    }
  },

  updateProgressBar: function(data, flowStartTime, flowLastTime) {
    if (data.startTime == -1) {
      return;
    }

    var outerWidth = $(".flow-progress").css("width");
    if (outerWidth) {
      if (outerWidth.substring(outerWidth.length - 2, outerWidth.length) == "px") {
        outerWidth = outerWidth.substring(0, outerWidth.length - 2);
      }
      outerWidth = parseInt(outerWidth);
    }

    var parentLastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
    var parentStartTime = data.startTime;

    var factor = outerWidth / (flowLastTime - flowStartTime);
    var outerProgressBarWidth = factor * (parentLastTime - parentStartTime);
    var outerLeftMargin = factor * (parentStartTime - flowStartTime);

    var nodes = data.nodes;
    for (var i = 0; i < nodes.length; ++i) {
      var node = nodes[i];

      // calculate the progress
      var tr = node.joblistrow;
      var outerProgressBar = $(tr).find("> td.timeline > .flow-progress");
      var progressBar = $(tr).find("> td.timeline > .flow-progress > .main-progress");
      var offsetLeft = 0;
      var minOffset = 0;
      progressBar.attempt = 0;

      // Shift the outer progress
      $(outerProgressBar).css("width", outerProgressBarWidth)
      $(outerProgressBar).css("margin-left", outerLeftMargin);

      // Add all the attempts
      if (node.pastAttempts) {
        var logURL = contextURL + "/executor?execid=" + execId + "&job=" + node.nestedId + "&attempt=" +  node.pastAttempts.length;
        var anchor = $(tr).find("> td.details > a");
        if (anchor.length != 0) {
          $(anchor).attr("href", logURL);
          progressBar.attempt = node.pastAttempts.length;
        }

        // Calculate the node attempt bars
        for (var p = 0; p < node.pastAttempts.length; ++p) {
          var pastAttempt = node.pastAttempts[p];
          var pastAttemptBox = pastAttempt.attemptBox;

          var left = (pastAttempt.startTime - flowStartTime)*factor;
          var width =  Math.max((pastAttempt.endTime - pastAttempt.startTime)*factor, 3);

          var margin = left - offsetLeft;
          $(pastAttemptBox).css("margin-left", left - offsetLeft);
          $(pastAttemptBox).css("width", width);

          $(pastAttemptBox).attr("title", "attempt:" + p + "  start:" + getHourMinSec(new Date(pastAttempt.startTime)) + "  end:" + getHourMinSec(new Date(pastAttempt.endTime)));
          offsetLeft += width + margin;
        }
      }

      var nodeLastTime = node.endTime == -1 ? (new Date()).getTime() : node.endTime;
      var left = Math.max((node.startTime-parentStartTime)*factor, minOffset);
      var margin = left - offsetLeft;
      var width = Math.max((nodeLastTime - node.startTime)*factor, 3);
      width = Math.min(width, outerWidth);

      progressBar.css("margin-left", left)
      progressBar.css("width", width);
      progressBar.attr("title", "attempt:" + progressBar.attempt + "  start:" + getHourMinSec(new Date(node.startTime)) + "  end:" + getHourMinSec(new Date(node.endTime)));

      if (node.nodes) {
        this.updateProgressBar(node, flowStartTime, flowLastTime);
      }
    }
  },

  toggleExpandFlow: function(flow) {
    console.log("Toggle Expand");
    var tr = flow.joblistrow;
    var subFlowRow = tr.subflowrow;
    var expandIcon = $(tr).find("> td > .listExpand");
    if (tr.expanded) {
      tr.expanded = false;
      $(expandIcon).removeClass("glyphicon-chevron-up");
      $(expandIcon).addClass("glyphicon-chevron-down");

      $(tr).removeClass("expanded");
      $(subFlowRow).hide();
    }
    else {
      tr.expanded = true;
      $(expandIcon).addClass("glyphicon-chevron-up");
      $(expandIcon).removeClass("glyphicon-chevron-down");
      $(tr).addClass("expanded");
      $(subFlowRow).show();
    }
  },

  addNodeRow: function(node, body) {
    var self = this;
    var tr = document.createElement("tr");
    var tdName = document.createElement("td");
    var tdType = document.createElement("td");
    var tdTimeline = document.createElement("td");
    var tdStart = document.createElement("td");
    var tdEnd = document.createElement("td");
    var tdElapse = document.createElement("td");
    var tdStatus = document.createElement("td");
    var tdDetails = document.createElement("td");
    node.joblistrow = tr;
    tr.node = node;
    var padding = 15*$(body)[0].level;

    $(tr).append(tdName);
    $(tr).append(tdType);
    $(tr).append(tdTimeline);
    $(tr).append(tdStart);
    $(tr).append(tdEnd);
    $(tr).append(tdElapse);
    $(tr).append(tdStatus);
    $(tr).append(tdDetails);
    $(tr).addClass("jobListRow");

    $(tdName).addClass("jobname");
    $(tdType).addClass("jobtype");
    if (padding) {
      $(tdName).css("padding-left", padding);
    }
    $(tdTimeline).addClass("timeline");
    $(tdStart).addClass("startTime");
    $(tdEnd).addClass("endTime");
    $(tdElapse).addClass("elapsedTime");
    $(tdStatus).addClass("statustd");
    $(tdDetails).addClass("details");

    $(tdType).text(node.type);

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

    var requestURL = contextURL + "/manager?project=" + projectName + "&job=" + node.id + "&history";
    var a = document.createElement("a");
    $(a).attr("href", requestURL);
    $(a).text(node.id);
    $(tdName).append(a);
    if (node.type=="flow") {
      var expandIcon = document.createElement("div");
      $(expandIcon).addClass("listExpand");
      $(tdName).append(expandIcon);
      $(expandIcon).addClass("expandarrow glyphicon glyphicon-chevron-down");
      $(expandIcon).click(function(evt) {
        var parent = $(evt.currentTarget).parents("tr")[0];
        self.toggleExpandFlow(parent.node);
      });
    }

    var status = document.createElement("div");
    $(status).addClass("status");
    //$(status).attr("id", node.id + "-status-div");
    tdStatus.appendChild(status);

    var logURL = contextURL + "/executor?execid=" + execId + "&job=" + node.nestedId;
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
    }
  }
});

var attemptRightClick = function(event) {
  var target = event.currentTarget;
  var job = target.job;
  var attempt = target.attempt;

  var jobId = event.currentTarget.jobid;
  var requestURL = contextURL + "/executor?project=" + projectName + "&execid=" + execId + "&job=" + job + "&attempt=" + attempt;

  var menu = [
    {title: "Open Attempt Log...", callback: function() {window.location.href=requestURL;}},
    {title: "Open Attempt Log in New Window...", callback: function() {window.open(requestURL);}}
  ];

  contextMenuView.show(event, menu);
  return false;
}

