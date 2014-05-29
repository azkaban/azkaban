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

var handleJobMenuClick = function(action, el, pos) {
  var jobid = el[0].jobid;
  var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" +
      flowId + "&job=" + jobid;
  if (action == "open") {
    window.location.href = requestURL;
  }
  else if (action == "openwindow") {
    window.open(requestURL);
  }
}

var flowTabView;
azkaban.FlowTabView = Backbone.View.extend({
  events: {
    "click #graphViewLink": "handleGraphLinkClick",
    "click #executionsViewLink": "handleExecutionLinkClick",
    "click #summaryViewLink": "handleSummaryLinkClick"
  },

  initialize: function(settings) {
    var selectedView = settings.selectedView;
    if (selectedView == "executions") {
      this.handleExecutionLinkClick();
    }
    else {
      this.handleGraphLinkClick();
    }
  },

  render: function() {
    console.log("render graph");
  },

  handleGraphLinkClick: function(){
    $("#executionsViewLink").removeClass("active");
    $("#graphViewLink").addClass("active");
    $('#summaryViewLink').removeClass('active');

    $("#executionsView").hide();
    $("#graphView").show();
    $('#summaryView').hide();
  },

  handleExecutionLinkClick: function() {
    $("#graphViewLink").removeClass("active");
    $("#executionsViewLink").addClass("active");
    $('#summaryViewLink').removeClass('active');

    $("#graphView").hide();
    $("#executionsView").show();
    $('#summaryView').hide();
    executionModel.trigger("change:view");
  },

  handleSummaryLinkClick: function() {
    $('#graphViewLink').removeClass('active');
    $('#executionsViewLink').removeClass('active');
    $('#summaryViewLink').addClass('active');

    $('#graphView').hide();
    $('#executionsView').hide();
    $('#summaryView').show();
  },
});

var jobListView;
var svgGraphView;
var executionsView;

azkaban.ExecutionsView = Backbone.View.extend({
  events: {
    "click #pageSelection li": "handleChangePageSelection"
  },

  initialize: function(settings) {
    this.model.bind('change:view', this.handleChangeView, this);
    this.model.bind('render', this.render, this);
    this.model.set({page: 1, pageSize: 16});
    this.model.bind('change:page', this.handlePageChange, this);
  },

  render: function(evt) {
    console.log("render");
    // Render page selections
    var tbody = $("#execTableBody");
    tbody.empty();

    var executions = this.model.get("executions");
    for (var i = 0; i < executions.length; ++i) {
      var row = document.createElement("tr");

      var tdId = document.createElement("td");
      var execA = document.createElement("a");
      $(execA).attr("href", contextURL + "/executor?execid=" + executions[i].execId);
      $(execA).text(executions[i].execId);
      tdId.appendChild(execA);
      row.appendChild(tdId);

      var tdUser = document.createElement("td");
      $(tdUser).text(executions[i].submitUser);
      row.appendChild(tdUser);

      var startTime = "-";
      if (executions[i].startTime != -1) {
        var startDateTime = new Date(executions[i].startTime);
        startTime = getDateFormat(startDateTime);
      }

      var tdStartTime = document.createElement("td");
      $(tdStartTime).text(startTime);
      row.appendChild(tdStartTime);

      var endTime = "-";
      var lastTime = executions[i].endTime;
      if (executions[i].endTime != -1) {
        var endDateTime = new Date(executions[i].endTime);
        endTime = getDateFormat(endDateTime);
      }
      else {
        lastTime = (new Date()).getTime();
      }

      var tdEndTime = document.createElement("td");
      $(tdEndTime).text(endTime);
      row.appendChild(tdEndTime);

      var tdElapsed = document.createElement("td");
      $(tdElapsed).text( getDuration(executions[i].startTime, lastTime));
      row.appendChild(tdElapsed);

      var tdStatus = document.createElement("td");
      var status = document.createElement("div");
      $(status).addClass("status");
      $(status).addClass(executions[i].status);
      $(status).text(statusStringMap[executions[i].status]);
      tdStatus.appendChild(status);
      row.appendChild(tdStatus);

      var tdAction = document.createElement("td");
      row.appendChild(tdAction);

      tbody.append(row);
    }

    this.renderPagination(evt);
  },

  renderPagination: function(evt) {
    var total = this.model.get("total");
    total = total? total : 1;
    var pageSize = this.model.get("pageSize");
    var numPages = Math.ceil(total / pageSize);

    this.model.set({"numPages": numPages});
    var page = this.model.get("page");

    //Start it off
    $("#pageSelection .active").removeClass("active");

    // Disable if less than 5
    console.log("Num pages " + numPages)
    var i = 1;
    for (; i <= numPages && i <= 5; ++i) {
      $("#page" + i).removeClass("disabled");
    }
    for (; i <= 5; ++i) {
      $("#page" + i).addClass("disabled");
    }

    // Disable prev/next if necessary.
    if (page > 1) {
      $("#previous").removeClass("disabled");
      $("#previous")[0].page = page - 1;
      $("#previous a").attr("href", "#page" + (page - 1));
    }
    else {
      $("#previous").addClass("disabled");
    }

    if (page < numPages) {
      $("#next")[0].page = page + 1;
      $("#next").removeClass("disabled");
      $("#next a").attr("href", "#page" + (page + 1));
    }
    else {
      $("#next")[0].page = page + 1;
      $("#next").addClass("disabled");
    }

    // Selection is always in middle unless at barrier.
    var startPage = 0;
    var selectionPosition = 0;
    if (page < 3) {
      selectionPosition = page;
      startPage = 1;
    }
    else if (page == numPages) {
      selectionPosition = 5;
      startPage = numPages - 4;
    }
    else if (page == numPages - 1) {
      selectionPosition = 4;
      startPage = numPages - 4;
    }
    else {
      selectionPosition = 3;
      startPage = page - 2;
    }

    $("#page"+selectionPosition).addClass("active");
    $("#page"+selectionPosition)[0].page = page;
    var selecta = $("#page" + selectionPosition + " a");
    selecta.text(page);
    selecta.attr("href", "#page" + page);

    for (var j = 0; j < 5; ++j) {
      var realPage = startPage + j;
      var elementId = "#page" + (j+1);

      $(elementId)[0].page = realPage;
      var a = $(elementId + " a");
      a.text(realPage);
      a.attr("href", "#page" + realPage);
    }
  },

  handleChangePageSelection: function(evt) {
    if ($(evt.currentTarget).hasClass("disabled")) {
      return;
    }
    var page = evt.currentTarget.page;
    this.model.set({"page": page});
  },

  handleChangeView: function(evt) {
    if (this.init) {
      return;
    }
    console.log("init");
    this.handlePageChange(evt);
    this.init = true;
  },

  handlePageChange: function(evt) {
    var page = this.model.get("page") - 1;
    var pageSize = this.model.get("pageSize");
    var requestURL = contextURL + "/manager";

    var model = this.model;
    var requestData = {
      "project": projectName,
      "flow": flowId,
      "ajax": "fetchFlowExecutions",
      "start": page * pageSize,
      "length": pageSize
    };
    var successHandler = function(data) {
      model.set({
        "executions": data.executions,
        "total": data.total
      });
      model.trigger("render");
    };
    $.get(requestURL, requestData, successHandler, "json");
  }
});

var summaryView;
azkaban.SummaryView = Backbone.View.extend({
  events: {
    'click #analyze-btn': 'fetchLastRun'
  },

  initialize: function(settings) {
    this.model.bind('change:view', this.handleChangeView, this);
    this.model.bind('render', this.render, this);

    this.fetchDetails();
    this.fetchSchedule();
    this.model.trigger('render');
  },

  fetchDetails: function() {
    var requestURL = contextURL + "/manager";
    var requestData = {
      'ajax': 'fetchflowdetails',
      'project': projectName,
      'flow': flowId
    };

    var model = this.model;

    var successHandler = function(data) {
      console.log(data);
      model.set({
        'jobTypes': data.jobTypes
      });
      model.trigger('render');
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchSchedule: function() {
    var requestURL = contextURL + "/schedule"
    var requestData = {
      'ajax': 'fetchSchedule',
      'projectId': projectId,
      'flowId': flowId
    };
    var model = this.model;
    var view = this;
    var successHandler = function(data) {
      model.set({'schedule': data.schedule});
      model.trigger('render');
      view.fetchSla();
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchSla: function() {
    var schedule = this.model.get('schedule');
    if (schedule == null || schedule.scheduleId == null) {
      return;
    }

    var requestURL = contextURL + "/schedule"
    var requestData = {
      "scheduleId": schedule.scheduleId,
      "ajax": "slaInfo"
    };
    var model = this.model;
    var successHandler = function(data) {
      if (data == null || data.settings == null || data.settings.length == 0) {
        return;
      }
      schedule.slaOptions = true;
      model.set({'schedule': schedule});
      model.trigger('render');
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchLastRun: function() {
    var requestURL = contextURL + "/manager";
    var requestData = {
      'ajax': 'fetchLastSuccessfulFlowExecution',
      'project': projectName,
      'flow': flowId
    };
    var view = this;
    var successHandler = function(data) {
      if (data.success == "false" || data.execId == null) {
        dust.render("flowstats-no-data", data, function(err, out) {
          $('#flow-stats-container').html(out);
        });
        return;
      }
      flowStatsView.show(data.execId);
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  handleChangeView: function(evt) {
  },

  render: function(evt) {
    var data = {
      projectName: projectName,
      flowName: flowId,
          jobTypes: this.model.get('jobTypes'),
      schedule: this.model.get('schedule'),
    };
    dust.render("flowsummary", data, function(err, out) {
      $('#summary-view-content').html(out);
    });
  },
});

var graphModel;
var mainSvgGraphView;

var executionModel;
azkaban.ExecutionModel = Backbone.Model.extend({});

var summaryModel;
azkaban.SummaryModel = Backbone.Model.extend({});

var flowStatsView;
var flowStatsModel;

var executionsTimeGraphView;
var slaView;

$(function() {
  var selected;
  // Execution model has to be created before the window switches the tabs.
  executionModel = new azkaban.ExecutionModel();
  executionsView = new azkaban.ExecutionsView({
    el: $('#executionsView'),
    model: executionModel
  });

  summaryModel = new azkaban.SummaryModel();
  summaryView = new azkaban.SummaryView({
    el: $('#summaryView'),
    model: summaryModel
  });

  flowStatsModel = new azkaban.FlowStatsModel();
  flowStatsView = new azkaban.FlowStatsView({
    el: $('#flow-stats-container'),
    model: flowStatsModel
  });

  flowTabView = new azkaban.FlowTabView({
    el: $('#headertabs'),
    selectedView: selected
  });

  graphModel = new azkaban.GraphModel();
  mainSvgGraphView = new azkaban.SvgGraphView({
    el: $('#svgDiv'),
    model: graphModel,
    rightClick: {
      "node": nodeClickCallback,
      "edge": edgeClickCallback,
      "graph": graphClickCallback
    }
  });

  jobsListView = new azkaban.JobListView({
    el: $('#joblist-panel'),
    model: graphModel,
    contextMenuCallback: jobClickCallback
  });

  executionsTimeGraphView = new azkaban.TimeGraphView({
    el: $('#timeGraph'),
    model: executionModel,
    modelField: 'executions'
  });

  slaView = new azkaban.ChangeSlaView({el:$('#sla-options')});

  var requestURL = contextURL + "/manager";
  // Set up the Flow options view. Create a new one every time :p
  $('#executebtn').click(function() {
    var data = graphModel.get("data");
    var nodes = data.nodes;
    var executingData = {
      project: projectName,
      ajax: "executeFlow",
      flow: flowId
    };

    flowExecuteDialogView.show(executingData);
  });

  var requestData = {
    "project": projectName,
    "ajax": "fetchflowgraph",
    "flow": flowId
  };
  var successHandler = function(data) {
    console.log("data fetched");
    graphModel.addFlow(data);
    graphModel.trigger("change:graph");

    // Handle the hash changes here so the graph finishes rendering first.
    if (window.location.hash) {
      var hash = window.location.hash;
      if (hash == "#executions") {
        flowTabView.handleExecutionLinkClick();
      }
      if (hash == "#summary") {
        flowTabView.handleSummaryLinkClick();
      }
      else if (hash == "#graph") {
        // Redundant, but we may want to change the default.
        selected = "graph";
      }
      else {
        if ("#page" == hash.substring(0, "#page".length)) {
          var page = hash.substring("#page".length, hash.length);
          console.log("page " + page);
          flowTabView.handleExecutionLinkClick();
          executionModel.set({"page": parseInt(page)});
        }
        else {
          selected = "graph";
        }
      }
    }
  };
  $.get(requestURL, requestData, successHandler, "json");
});
