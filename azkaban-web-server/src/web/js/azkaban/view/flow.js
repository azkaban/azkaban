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

var handleJobMenuClick = function (action, el, pos) {
  var jobid = el[0].jobid;
  var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" +
      flowId + "&job=" + jobid;
  if (action == "open") {
    window.location.href = requestURL;
  }
  else if (action == "openwindow") {
    window.open(requestURL);
  }
};

var initPagination = function (elem, model) {
  var totalPages = model.get("total");
  if (!totalPages) {
    return;
  }

  $(elem).twbsPagination({
    totalPages: Math.ceil(totalPages / model.get("pageSize")),
    startPage: model.get("page"),
    initiateStartPageClick: false,
    visiblePages: model.get("visiblePages"),
    onPageClick: function (event, page) {
      model.set({"page": page});
    }
  });
};

var flowTabView;
azkaban.FlowTabView = Backbone.View.extend({
  events: {
    "click #graphViewLink": "handleGraphLinkClick",
    "click #executionsViewLink": "handleExecutionLinkClick",
    "click #flowtriggersViewLink": "handleFlowTriggerLinkClick",
    "click #summaryViewLink": "handleSummaryLinkClick"
  },

  initialize: function (settings) {
    var selectedView = settings.selectedView;
    if (selectedView == "executions") {
      this.handleExecutionLinkClick();
    }
    else {
      this.handleGraphLinkClick();
    }
  },

  render: function () {
    console.log("render graph");
  },

  handleGraphLinkClick: function () {
    $("#executionsViewLink").removeClass("active");
    $("#graphViewLink").addClass("active");
    $("#flowtriggersViewLink").removeClass("active");
    $('#summaryViewLink').removeClass('active');

    $("#graphView").show();
    $("#flowtriggerView").hide();
    $("#executionsView").hide();
    $('#summaryView').hide();
  },

  handleExecutionLinkClick: function () {
    $("#graphViewLink").removeClass("active");
    $("#executionsViewLink").addClass("active");
    $("#flowtriggersViewLink").removeClass("active");
    $('#summaryViewLink').removeClass('active');

    $("#graphView").hide();
    $("#flowtriggerView").hide();
    $("#executionsView").show();
    $('#summaryView').hide();
    executionModel.trigger("initView");
  },

  handleFlowTriggerLinkClick: function () {
    $("#graphViewLink").removeClass("active");
    $("#executionsViewLink").removeClass("active");
    $("#flowtriggersViewLink").addClass("active");
    $('#summaryViewLink').removeClass('active');

    $("#graphView").hide();
    $("#flowtriggerView").show();
    $("#executionsView").hide();
    $('#summaryView').hide();
    flowTriggerModel.trigger("initView");
  },

  handleSummaryLinkClick: function () {
    $('#graphViewLink').removeClass('active');
    $('#executionsViewLink').removeClass('active');
    $("#flowtriggersViewLink").removeClass("active");
    $('#summaryViewLink').addClass('active');

    $('#graphView').hide();
    $("#flowtriggerView").hide();
    $('#executionsView').hide();
    $('#summaryView').show();
  },
});

var jobListView;
var svgGraphView;

var executionModel;
azkaban.ExecutionModel = Backbone.Model.extend({});

var executionsView;
azkaban.ExecutionsView = Backbone.View.extend({

  initialize: function (settings) {
    // Interested on first tab activation only to load init data
    this.model.once('initView', this.handleInitView, this);
    this.model.bind('render', this.render, this);
    this.model.bind('change:page', this.handlePageChange, this);
  },

  render: function () {
    // Render page selections
    var tbody = $("#execTableBody");
    tbody.empty();

    var executions = this.model.get("executions");
    for (var i = 0; i < executions.length; ++i) {
      var row = document.createElement("tr");

      var tdId = document.createElement("td");
      var execA = document.createElement("a");
      $(execA).attr("href", this.model.get("executionUrl") + "?execid="
          + executions[i].execId);
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
      if (executions[i].endTime != -1 && executions[i].endTime != 0) {
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
      $(tdElapsed).text(getDuration(executions[i].startTime, lastTime));
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
  },

  handleInitView: function () {
    var model = this.model;
    this.loadAndRenderData(function () {
      initPagination('#executionsPagination', model);
    });
  },

  handlePageChange: function () {
    this.loadAndRenderData();
  },

  loadAndRenderData: function (onDataLoadedCb) {
    var model = this.model;
    var currentPage = model.get("page");
    var pageSize = model.get("pageSize");
    var requestURL = model.get("fetchFlowExecutionsUrl");
    var requestData = {
      "project": model.get("projectName"),
      "flow": model.get("flowId"),
      "ajax": "fetchFlowExecutions",
      "start": (currentPage - 1) * pageSize,
      "length": pageSize
    };

    $.get(requestURL, requestData)
    .done(function (data) {
      model.set({
        "executions": data.executions,
        "total": data.total
      });
      model.trigger("render");
      if (onDataLoadedCb) {
        onDataLoadedCb();
      }
    })
    .fail(function () {
      // TODO
    });
  }
});

var flowTriggerModel;
azkaban.FlowTriggerModel = Backbone.Model.extend({});

var flowTriggersView;
azkaban.FlowTriggersView = Backbone.View.extend({

  initialize: function (settings) {
    // Interested on first tab activation only to load init data
    this.model.once('initView', this.handleInitView, this);
    this.model.bind('render', this.render, this);
    this.model.bind('change:page', this.handlePageChange, this);
  },

  render: function () {
    // Render page selections
    var tbody = $("#flowtriggerTableBody");
    tbody.empty();

    var executions = this.model.get("executions");
    for (var i = 0; i < executions.length; ++i) {
      var row = document.createElement("tr");

      var tdId = document.createElement("td");
      var execA = document.createElement("a");
      $(execA).attr("href", this.model.get("triggerInstanceUrl") +
          "?triggerinstanceid=" + executions[i].instanceId);
      $(execA).text(executions[i].instanceId);
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
      if (executions[i].endTime != -1 && executions[i].endTime != 0) {
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
      $(tdElapsed).text(getDuration(executions[i].startTime, lastTime));
      row.appendChild(tdElapsed);

      var tdStatus = document.createElement("td");
      var status = document.createElement("div");
      $(status).addClass("status");
      $(status).addClass(executions[i].status);
      $(status).text(executions[i].status);
      tdStatus.appendChild(status);
      row.appendChild(tdStatus);

      var tdAction = document.createElement("td");
      row.appendChild(tdAction);

      tbody.append(row);
    }
  },

  handleInitView: function () {
    var model = this.model;
    this.loadAndRenderData(function () {
      initPagination('#flowtriggerPagination', model);
    });
  },

  handlePageChange: function () {
    this.loadAndRenderData();
  },

  loadAndRenderData: function (onDataLoadedCb) {
    var model = this.model;
    var currentPage = model.get("page");
    var pageSize = model.get("pageSize");
    var requestURL = model.get("fetchTriggerInstancesUrl");
    var requestData = {
      "project": model.get("projectName"),
      "flow": model.get("flowId"),
      "ajax": "fetchTriggerInstances",
      "start": (currentPage - 1) * pageSize,
      "length": pageSize
    };

    $.get(requestURL, requestData)
    .done(function (data) {
      model.set({
        "executions": data.executions,
        "total": data.total
      });
      model.trigger("render");
      if (onDataLoadedCb) {
        onDataLoadedCb();
      }
    })
    .fail(function () {
      // TODO
    });
  }
});

var summaryModel;
azkaban.SummaryModel = Backbone.Model.extend({});

var summaryView;
azkaban.SummaryView = Backbone.View.extend({
  events: {
    'click #analyze-btn': 'fetchLastRun'
  },

  initialize: function (settings) {
    this.model.bind('change:view', this.handleChangeView, this);
    this.model.bind('render', this.render, this);

    this.fetchDetails();
    this.fetchSchedule();
    this.fetchFlowTrigger();
    this.model.trigger('render');
  },

  fetchDetails: function () {
    var model = this.model;
    var requestURL = model.get("fetchFlowDetailsUrl");
    var requestData = {
      'ajax': 'fetchflowdetails',
      'project': model.get("projectName"),
      'flow': model.get("flowId")
    };

    var successHandler = function (data) {
      console.log(data);
      model.set({
        jobTypes: data.jobTypes,
        condition: data.condition
      });
      model.trigger('render');
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchSchedule: function () {
    var model = this.model;
    var requestURL = model.get("fetchScheduleUrl");
    var requestData = {
      'ajax': 'fetchSchedule',
      'projectId': model.get("projectId"),
      'flowId': model.get("flowId")
    };

    var view = this;
    var successHandler = function (data) {
      model.set({'schedule': data.schedule});
      model.trigger('render');
      view.fetchSla();
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchSla: function () {
    var schedule = this.model.get('schedule');
    if (schedule == null || schedule.scheduleId == null) {
      return;
    }

    var model = this.model;
    var requestURL = model.get("slaInfoUrl");
    var requestData = {
      "scheduleId": schedule.scheduleId,
      "ajax": "slaInfo"
    };

    var successHandler = function (data) {
      if (data == null || data.settings == null || data.settings.length == 0) {
        return;
      }
      schedule.slaOptions = true;
      model.set({'schedule': schedule});
      model.trigger('render');
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchLastRun: function () {
    var model = this.model;
    var requestURL = model.get("fetchLastSuccessfulFlowExecutionUrl");
    var requestData = {
      'ajax': 'fetchLastSuccessfulFlowExecution',
      'project': model.get("projectName"),
      'flow': model.get("flowId")
    };
    var view = this;
    var successHandler = function (data) {
      if (data.success == "false" || data.execId == null) {
        dust.render("flowstats-no-data", data, function (err, out) {
          $('#flow-stats-container').html(out);
        });
        return;
      }
      flowStatsView.show(data.execId);
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  fetchFlowTrigger: function () {
    var model = this.model;
    var requestURL = model.get("fetchTriggerUrl");
    var requestData = {
      'ajax': 'fetchTrigger',
      'projectId': model.get("projectId"),
      'flowId': model.get("flowId")
    };

    var successHandler = function (data) {
      model.set({'flowtrigger': data.flowTrigger});
      model.trigger('render');
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },

  handleChangeView: function (evt) {
  },

  render: function (evt) {
    var model = this.model;
    var data = {
      projectName: model.get('projectName'),
      flowName: model.get('flowId'),
      jobTypes: model.get('jobTypes'),
      condition: model.get('condition'),
      schedule: model.get('schedule'),
      flowtrigger: model.get('flowtrigger')
    };
    dust.render("flowsummary", data, function (err, out) {
      $('#summary-view-content').html(out);
    });
  },
});

var graphModel;
var mainSvgGraphView;
var flowStatsView;
var flowStatsModel;
var executionsTimeGraphView;
var slaView;

var initFlowPage = function (settings) {
  var selected;

  // Execution model has to be created before the window switches the tabs.
  executionModel = new azkaban.ExecutionModel({
    page: 1,
    pageSize: settings.pageSize,
    visiblePages: 5,
    projectName: settings.projectName,
    flowId: settings.flowId,
    executionUrl: settings.executionUrl,
    fetchFlowExecutionsUrl: settings.fetchFlowExecutionsUrl
  });
  executionsView = new azkaban.ExecutionsView({
    el: $('#executionsView'),
    model: executionModel
  });

  flowTriggerModel = new azkaban.FlowTriggerModel({
    page: 1,
    pageSize: settings.pageSize,
    visiblePages: 5,
    projectName: settings.projectName,
    flowId: settings.flowId,
    triggerInstanceUrl: settings.triggerInstanceUrl,
    fetchTriggerInstancesUrl: settings.fetchTriggerInstancesUrl
  });
  flowTriggersView = new azkaban.FlowTriggersView({
    el: $('#flowtriggerView'),
    model: flowTriggerModel
  });

  summaryModel = new azkaban.SummaryModel({
    projectId: settings.projectId,
    projectName: settings.projectName,
    flowId: settings.flowId,
    fetchFlowDetailsUrl: settings.fetchFlowDetailsUrl,
    fetchScheduleUrl: settings.fetchScheduleUrl,
    slaInfoUrl: settings.slaInfoUrl,
    fetchLastSuccessfulFlowExecutionUrl: settings.fetchLastSuccessfulFlowExecutionUrl,
    fetchTriggerUrl: settings.fetchTriggerUrl
  });
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

  slaView = new azkaban.ChangeSlaView({el: $('#sla-options')});

  // Set up the Flow options view. Create a new one every time :p
  $('#executebtn').click(function () {
    var executingData = {
      project: settings.projectName,
      ajax: "executeFlow",
      flow: settings.flowId,
      flowDisplayName: settings.flowDisplayName
    };

    flowExecuteDialogView.show(executingData);
  });

  var requestData = {
    "project": settings.projectName,
    "ajax": "fetchflowgraph",
    "flow": settings.flowId
  };
  var successHandler = function (data) {
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
      if (hash == "#flowtriggers") {
        flowTabView.handleFlowTriggerLinkClick();
      }
      if (hash == "#graph") {
        // Redundant, but we may want to change the default.
        selected = "graph";
      }
    }
  };
  $.get(settings.fetchFlowGraphUrl, requestData, successHandler, "json");
};
