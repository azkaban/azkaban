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
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowName + "&job=" + jobid;
	if (action == "open") {
		window.location.href = requestURL;
	}
	else if(action == "openwindow") {
		window.open(requestURL);
	}
}

var statusView;
azkaban.StatusView = Backbone.View.extend({
	initialize: function(settings) {
		this.model.bind('change:graph', this.render, this);
		this.model.bind('change:update', this.statusUpdate, this);
	},
	render: function(evt) {
		var data = this.model.get("data");

		var user = data.submitUser;
		$("#submitUser").text(user);

		this.statusUpdate(evt);
	},

	statusUpdate: function(evt) {
		var data = this.model.get("data");

		statusItem = $("#flowStatus");
		for (var j = 0; j < statusList.length; ++j) {
			var status = statusList[j];
			statusItem.removeClass(status);
		}
		$("#flowStatus").addClass(data.status);
		$("#flowStatus").text(data.status);

		var startTime = data.startTime;
		var endTime = data.endTime;

		if (!startTime || startTime == -1) {
			$("#startTime").text("-");
		}
		else {
			var date = new Date(startTime);
			$("#startTime").text(getDateFormat(date));

			var lastTime = endTime;
			if (endTime == -1) {
				var currentDate = new Date();
				lastTime = currentDate.getTime();
			}

			var durationString = getDuration(startTime, lastTime);
			$("#duration").text(durationString);
		}

		if (!endTime || endTime == -1) {
			$("#endTime").text("-");
		}
		else {
			var date = new Date(endTime);
			$("#endTime").text(getDateFormat(date));
		}
	}
});

var flowTabView;
azkaban.FlowTabView = Backbone.View.extend({
	events: {
		"click #graphViewLink": "handleGraphLinkClick",
		"click #jobslistViewLink": "handleJobslistLinkClick",
		"click #flowLogViewLink": "handleLogLinkClick",
		"click #statsViewLink": "handleStatsLinkClick",
		"click #cancelbtn": "handleCancelClick",
		"click #executebtn": "handleRestartClick",
		"click #pausebtn": "handlePauseClick",
		"click #resumebtn": "handleResumeClick",
		"click #retrybtn": "handleRetryClick"
	},

	initialize: function(settings) {
		$("#cancelbtn").hide();
		$("#executebtn").hide();
		$("#pausebtn").hide();
		$("#resumebtn").hide();
		$("#retrybtn").hide();

		this.model.bind('change:graph', this.handleFlowStatusChange, this);
		this.model.bind('change:update', this.handleFlowStatusChange, this);

		var selectedView = settings.selectedView;
		if (selectedView == "jobslist") {
			this.handleJobslistLinkClick();
		}
		else {
			this.handleGraphLinkClick();
		}
	},

	render: function() {
		console.log("render graph");
	},

	handleGraphLinkClick: function(){
		$("#jobslistViewLink").removeClass("active");
		$("#graphViewLink").addClass("active");
		$("#flowLogViewLink").removeClass("active");
		$("#statsViewLink").removeClass("active");

		$("#jobListView").hide();
		$("#graphView").show();
		$("#flowLogView").hide();
		$("#statsView").hide();
	},

	handleJobslistLinkClick: function() {
		$("#graphViewLink").removeClass("active");
		$("#jobslistViewLink").addClass("active");
		$("#flowLogViewLink").removeClass("active");
		$("#statsViewLink").removeClass("active");

		$("#graphView").hide();
		$("#jobListView").show();
		$("#flowLogView").hide();
		$("#statsView").hide();
	},

	handleLogLinkClick: function() {
		$("#graphViewLink").removeClass("active");
		$("#jobslistViewLink").removeClass("active");
		$("#flowLogViewLink").addClass("active");
		$("#statsViewLink").removeClass("active");

		$("#graphView").hide();
		$("#jobListView").hide();
		$("#flowLogView").show();
		$("#statsView").hide();
	},

  handleStatsLinkClick: function() {
		$("#graphViewLink").removeClass("active");
		$("#jobslistViewLink").removeClass("active");
		$("#flowLogViewLink").removeClass("active");
		$("#statsViewLink").addClass("active");

		$("#graphView").hide();
		$("#jobListView").hide();
		$("#flowLogView").hide();
    statsView.show();
		$("#statsView").show();
	},

	handleFlowStatusChange: function() {
		var data = this.model.get("data");
		$("#cancelbtn").hide();
		$("#executebtn").hide();
		$("#pausebtn").hide();
		$("#resumebtn").hide();
		$("#retrybtn").hide();

		if (data.status == "SUCCEEDED") {
      $("#executebtn").show();
		}
		else if (data.status == "FAILED") {
			$("#executebtn").show();
		}
		else if (data.status == "FAILED_FINISHING") {
			$("#cancelbtn").show();
			$("#executebtn").hide();
			$("#retrybtn").show();
		}
		else if (data.status == "RUNNING") {
			$("#cancelbtn").show();
			$("#pausebtn").show();
		}
		else if (data.status == "PAUSED") {
			$("#cancelbtn").show();
			$("#resumebtn").show();
		}
		else if (data.status == "WAITING") {
			$("#cancelbtn").show();
		}
		else if (data.status == "KILLED") {
			$("#executebtn").show();
		}
	},

	handleCancelClick: function(evt) {
		var requestURL = contextURL + "/executor";
		var requestData = {"execid": execId, "ajax": "cancelFlow"};
		var successHandler = function(data) {
			console.log("cancel clicked");
			if (data.error) {
				showDialog("Error", data.error);
			}
			else {
				showDialog("Cancelled", "Flow has been cancelled.");
				setTimeout(function() {updateStatus();}, 1100);
			}
		};
		ajaxCall(requestURL, requestData, successHandler);
	},

	handleRetryClick: function(evt) {
		var graphData = graphModel.get("data");
		var requestURL = contextURL + "/executor";
		var requestData = {"execid": execId, "ajax": "retryFailedJobs"};
		var successHandler = function(data) {
			console.log("cancel clicked");
			if (data.error) {
				showDialog("Error", data.error);
			}
			else {
				showDialog("Retry", "Flow has been retried.");
				setTimeout(function() {updateStatus();}, 1100);
			}
		};
		ajaxCall(requestURL, requestData, successHandler);
	},

	handleRestartClick: function(evt) {
		console.log("handleRestartClick");
		var data = graphModel.get("data");

		var executingData = {
			project: projectName,
			ajax: "executeFlow",
			flow: flowId,
			execid: execId,
			exgraph: data
		};
		flowExecuteDialogView.show(executingData);
	},

	handlePauseClick: function(evt) {
		var requestURL = contextURL + "/executor";
		var requestData = {"execid": execId, "ajax":"pauseFlow"};
		var successHandler = function(data) {
			console.log("pause clicked");
			if (data.error) {
				showDialog("Error", data.error);
			}
			else {
				showDialog("Paused", "Flow has been paused.");
				setTimeout(function() {updateStatus();}, 1100);
			}
		};
		ajaxCall(requestURL, requestData, successHandler);
	},

	handleResumeClick: function(evt) {
		var requestURL = contextURL + "/executor";
		var requestData = {"execid": execId, "ajax":"resumeFlow"};
		var successHandler = function(data) {
			console.log("pause clicked");
			if (data.error) {
				showDialog("Error", data.error);
			}
			else {
				showDialog("Resumed", "Flow has been resumed.");
				setTimeout(function() {updateStatus();}, 1100);
			}
		};
		ajaxCall(requestURL, requestData, successHandler);
	}
});

var showDialog = function(title, message) {
	$('#messageTitle').text(title);
	$('#messageBox').text(message);
	$('#messageDialog').modal();
}

var jobListView;
var mainSvgGraphView;

var flowLogView;
azkaban.FlowLogView = Backbone.View.extend({
	events: {
		"click #updateLogBtn" : "handleUpdate"
	},
	initialize: function(settings) {
		this.model.set({"offset": 0});
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var offset = this.model.get("offset");
		var requestURL = contextURL + "/executor";
		var model = this.model;
		console.log("fetchLogs offset is " + offset)

		$.ajax({
			async: false,
			url: requestURL,
			data: {
				"execid": execId,
				"ajax": "fetchExecFlowLogs",
				"offset": offset,
				"length": 50000
			},
			success: function(data) {
				console.log("fetchLogs");
				if (data.error) {
					console.log(data.error);
				}
				else {
					var log = $("#logSection").text();
					if (!log) {
						log = data.data;
					}
					else {
						log += data.data;
					}

					var newOffset = data.offset + data.length;

					$("#logSection").text(log);
					model.set({"offset": newOffset, "log": log});
					$(".logViewer").scrollTop(9999);
				}
			}
		});
	}
});

var statsView;
azkaban.StatsView = Backbone.View.extend({
	events: {
	},

  initialize: function(settings) {
    this.model.bind('change:graph', this.statusUpdate, this);
    this.model.bind('change:update', this.statusUpdate, this);
		this.model.bind('render', this.render, this);
    this.status = null;
    this.rendered = false;
  },

  statusUpdate: function(evt) {
    var data = this.model.get('data');
    this.status = data.status;
  },

  show: function() {
    this.model.trigger("render");
  },

  render: function(evt) {
    if (this.rendered == true) {
      return;
    }
    if (this.status != 'SUCCEEDED') {
      return;
    }
    flowStatsView.show(execId);
    this.rendered = true;
  }
});

var graphModel;

var logModel;
azkaban.LogModel = Backbone.Model.extend({});

var updateStatus = function(updateTime) {
	var requestURL = contextURL + "/executor";
	var oldData = graphModel.get("data");
	var nodeMap = graphModel.get("nodeMap");

	if (!updateTime) {
		updateTime = oldData.updateTime ? oldData.updateTime : 0;
	}

	var requestData = {
		"execid": execId,
		"ajax": "fetchexecflowupdate",
		"lastUpdateTime": updateTime
	};

	var successHandler = function(data) {
		console.log("data updated");
		if (data.updateTime) {
			updateGraph(oldData, data);

			graphModel.set({"update": data});
			graphModel.trigger("change:update");
		}
	};
	ajaxCall(requestURL, requestData, successHandler);
}

var updateGraph = function(data, update) {
	var nodeMap = data.nodeMap;
	data.startTime = update.startTime;
	data.endTime = update.endTime;
	data.updateTime = update.updateTime;
	data.status = update.status;
	update.changedNode = data;

	if (update.nodes) {
		for (var i = 0; i < update.nodes.length; ++i) {
			var newNode = update.nodes[i];
			var oldNode = nodeMap[newNode.id];

			updateGraph(oldNode, newNode);
		}
	}
}

var updateTime = -1;
var updaterFunction = function() {
	var oldData = graphModel.get("data");
	var keepRunning =
			oldData.status != "SUCCEEDED" &&
			oldData.status != "FAILED" &&
			oldData.status != "KILLED";

	if (keepRunning) {
		updateStatus();

		var data = graphModel.get("data");
		if (data.status == "UNKNOWN" ||
			data.status == "WAITING" ||
			data.status == "PREPARING") {
			setTimeout(function() {updaterFunction();}, 1000);
		}
		else if (data.status != "SUCCEEDED" && data.status != "FAILED") {
			// 5 sec updates
			setTimeout(function() {updaterFunction();}, 5000);
		}
		else {
			console.log("Flow finished, so no more updates");
			setTimeout(function() {updateStatus(0);}, 500);
		}
	}
	else {
		console.log("Flow finished, so no more updates");
	}
}

var logUpdaterFunction = function() {
	var oldData = graphModel.get("data");
	var keepRunning =
			oldData.status != "SUCCEEDED" &&
			oldData.status != "FAILED" &&
			oldData.status != "KILLED";
	if (keepRunning) {
		// update every 30 seconds for the logs until finished
		flowLogView.handleUpdate();
		setTimeout(function() {logUpdaterFunction();}, 30000);
	}
	else {
		flowLogView.handleUpdate();
	}
}

var exNodeClickCallback = function(event) {
	console.log("Node clicked callback");
	var jobId = event.currentTarget.jobid;
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobId;
	var visualizerURL = contextURL + "/pigvisualizer?execid=" + execId + "&jobid=" + jobId;

	var menu = [
		{title: "Open Job...", callback: function() {window.location.href = requestURL;}},
		{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
		{title: "Visualize Job...", callback: function() {window.location.href = visualizerURL;}}
	];

	contextMenuView.show(event, menu);
}

var exJobClickCallback = function(event) {
	console.log("Node clicked callback");
	var jobId = event.currentTarget.jobid;
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobId;
	var visualizerURL = contextURL + "/pigvisualizer?execid=" + execId + "&jobid=" + jobId;

	var menu = [
		{title: "Open Job...", callback: function() {window.location.href = requestURL;}},
		{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
		{title: "Visualize Job...", callback: function() {window.location.href = visualizerURL;}}
	];

	contextMenuView.show(event, menu);
}

var exEdgeClickCallback = function(event) {
	console.log("Edge clicked callback");
}

var exGraphClickCallback = function(event) {
	console.log("Graph clicked callback");
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId;

	var menu = [
		{title: "Open Flow...", callback: function() {window.location.href=requestURL;}},
		{title: "Open Flow in New Window...", callback: function() {window.open(requestURL);}},
		{break: 1},
		{title: "Center Graph", callback: function() {graphModel.trigger("resetPanZoom");}}
	];

	contextMenuView.show(event, menu);
}

var flowStatsView;
var flowStatsModel;

$(function() {
	var selected;

	graphModel = new azkaban.GraphModel();
	logModel = new azkaban.LogModel();

	flowTabView = new azkaban.FlowTabView({
		el: $('#headertabs'),
		model: graphModel
	});

	mainSvgGraphView = new azkaban.SvgGraphView({
		el: $('#svgDiv'),
		model: graphModel,
		rightClick:	{
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

	flowLogView = new azkaban.FlowLogView({
		el: $('#flowLogView'),
		model: logModel
	});

	statusView = new azkaban.StatusView({
		el: $('#flow-status'),
		model: graphModel
	});

  flowStatsModel = new azkaban.FlowStatsModel();
	flowStatsView = new azkaban.FlowStatsView({
		el: $('#flow-stats-container'),
		model: flowStatsModel,
    histogram: false
	});

  statsView = new azkaban.StatsView({
		el: $('#statsView'),
		model: graphModel
	});

	executionListView = new azkaban.ExecutionListView({
		el: $('#jobListView'),
		model: graphModel
	});

	var requestURL = contextURL + "/executor";
	var requestData = {"execid": execId, "ajax":"fetchexecflow"};
	var successHandler = function(data) {
		console.log("data fetched");
		graphModel.addFlow(data);
		graphModel.trigger("change:graph");

		updateTime = Math.max(updateTime, data.submitTime);
		updateTime = Math.max(updateTime, data.startTime);
		updateTime = Math.max(updateTime, data.endTime);

		if (window.location.hash) {
			var hash = window.location.hash;
			if (hash == "#jobslist") {
				flowTabView.handleJobslistLinkClick();
			}
			else if (hash == "#log") {
				flowTabView.handleLogLinkClick();
			}
			else if (hash == "#stats") {
				flowTabView.handleStatsLinkClick();
			}
		}
		else {
			flowTabView.handleGraphLinkClick();
		}
		updaterFunction();
		logUpdaterFunction();
	};
	ajaxCall(requestURL, requestData, successHandler);
});
