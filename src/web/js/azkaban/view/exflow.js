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

var executionListView;
azkaban.ExecutionListView = Backbone.View.extend({
	events: {
		//"click .flow-progress-bar": "handleProgressBoxClick"
	},
	
	initialize: function(settings) {
		this.model.bind('change:graph', this.renderJobs, this);
		this.model.bind('change:update', this.updateJobs, this);
	},
	
	renderJobs: function(evt) {
		var data = this.model.get("data");
		var lastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
		this.updateJobRow(data.nodes);
		this.updateProgressBar(data);
	},

	/*handleProgressBoxClick: function(evt) {
		var target = evt.currentTarget;
		var job = target.job;
		var attempt = target.attempt;
		
		var data = this.model.get("data");
		var node = data.nodes[job];
		
		var jobId = event.currentTarget.jobid;
		var requestURL = contextURL + "/manager?project=" + projectName + "&execid=" + execId + "&job=" + job + "&attempt=" + attempt;
	
		var menu = [	
				{title: "Open Job...", callback: function() {window.location.href=requestURL;}},
				{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}}
		];
	
		contextMenuView.show(evt, menu);
	},*/
	
	updateJobs: function(evt) {
		var data = this.model.get("update");
		var lastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
		
		this.updateJobRow(data.nodes);
		this.updateProgressBar(this.model.get("data"));
	},
	
	updateJobRow: function(nodes) {
		if (!nodes) {
			return;
		}
		
		var executingBody = $("#executableBody");
		nodes.sort(function(a,b) { return a.startTime - b.startTime; });
		
		for (var i = 0; i < nodes.length; ++i) {
			var node = nodes[i];
			if (node.startTime < 0) {
				continue;
			}
			var nodeId = node.id.replace(".", "\\\\.");
			var row = document.getElementById(nodeId + "-row");
			if (!row) {
				this.addNodeRow(node);
			}
			
			var div = $("#" + nodeId + "-status-div");
			div.text(statusStringMap[node.status]);
			$(div).attr("class", "status " + node.status);
	
			var startdate = new Date(node.startTime);
			$("#" + nodeId + "-start").text(getDateFormat(startdate));
	  
			var endTime = node.endTime;
			if (node.endTime == -1) {
				$("#" + nodeId + "-end").text("-");
				endTime = node.startTime + 1;
			}
			else {
				var enddate = new Date(node.endTime);
				$("#" + nodeId + "-end").text(getDateFormat(enddate));
			}
	  
			var progressBar = $("#" + nodeId + "-progressbar");
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
					var attemptBarId = nodeId + "-progressbar-" + a;
					var attempt = node.pastAttempts[a];
					if ($("#" + attemptBarId).length == 0) {
						var attemptBox = document.createElement("div");
						$(attemptBox).attr("id", attemptBarId);
						$(attemptBox).addClass("flow-progress-bar");
						$(attemptBox).addClass("attempt");
						$(attemptBox).addClass(attempt.status);
						$(attemptBox).css("float","left");
						$(attemptBox).bind("contextmenu", attemptRightClick);
						$(progressBar).before(attemptBox);
						attemptBox.job = nodeId;
						attemptBox.attempt = a;
					}
				}
			}
  
			if (node.endTime == -1) {
				//$("#" + node.id + "-elapse").text("0 sec");
				$("#" + nodeId + "-elapse").text(getDuration(node.startTime, (new Date()).getTime()));					
			}
			else {
				$("#" + nodeId + "-elapse").text(getDuration(node.startTime, node.endTime));
			}
		}
	},
	
	updateProgressBar: function(data) {
		if (data.startTime == -1) {
			return;
		}
		
		var flowLastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
		var flowStartTime = data.startTime;

		var outerWidth = $(".flow-progress").css("width");
		if (outerWidth) {
			if (outerWidth.substring(outerWidth.length - 2, outerWidth.length) == "px") {
				outerWidth = outerWidth.substring(0, outerWidth.length - 2);
			}
			outerWidth = parseInt(outerWidth);
		}
		
		var nodes = data.nodes;
		var diff = flowLastTime - flowStartTime;
		var factor = outerWidth/diff;
		for (var i = 0; i < nodes.length; ++i) {
			var node = nodes[i];
			var nodeId = node.id.replace(".", "\\\\.");
			// calculate the progress

			var elem = $("#" + node.id + "-progressbar");
			var offsetLeft = 0;
			var minOffset = 0;
			elem.attempt = 0;
			
			// Add all the attempts
			if (node.pastAttempts) {
				var logURL = contextURL + "/executor?execid=" + execId + "&job=" + node.id + "&attempt=" +	node.pastAttempts.length;
				var aId = node.id + "-log-link";
				$("#" + aId).attr("href", logURL);
				elem.attempt = node.pastAttempts.length;
				
				// Calculate the node attempt bars
				for (var p = 0; p < node.pastAttempts.length; ++p) {
					var pastAttempt = node.pastAttempts[p];
					var pastAttemptBox = $("#" + nodeId + "-progressbar-" + p);
					
					var left = (pastAttempt.startTime - flowStartTime)*factor;
					var width =	Math.max((pastAttempt.endTime - pastAttempt.startTime)*factor, 3);
					
					var margin = left - offsetLeft;
					$(pastAttemptBox).css("margin-left", left - offsetLeft);
					$(pastAttemptBox).css("width", width);
					
					$(pastAttemptBox).attr("title", "attempt:" + p + "	start:" + getHourMinSec(new Date(pastAttempt.startTime)) + "	end:" + getHourMinSec(new Date(pastAttempt.endTime)));
					offsetLeft += width + margin;
				}
			}
			
			var nodeLastTime = node.endTime == -1 ? (new Date()).getTime() : node.endTime;
			var left = Math.max((node.startTime-flowStartTime)*factor, minOffset);
			var margin = left - offsetLeft;
			var width = Math.max((nodeLastTime - node.startTime)*factor, 3);
			width = Math.min(width, outerWidth);
			
			elem.css("margin-left", left)
			elem.css("width", width);
			elem.attr("title", "attempt:" + elem.attempt + "	start:" + getHourMinSec(new Date(node.startTime)) + "	end:" + getHourMinSec(new Date(node.endTime)));
		}
	},
	toggleExpandFlow: function(flow) {
		console.log("Toggle Expand");
		var tr = flow.progressbar;
		var expandIcon = $(tr).find("> td > .listExpand");
		if (tr.expanded) {
			tr.expanded = false;
			$(expandIcon).removeClass("glyphicon-chevron-up");
			$(expandIcon).addClass("glyphicon-chevron-down");
		}
		else {
			tr.expanded = true;
			$(expandIcon).addClass("glyphicon-chevron-up");
			$(expandIcon).removeClass("glyphicon-chevron-down");
		}
	},
	expandFlow: function(flow) {
		for (var i = 0; i < flow.nodes.length; ++i) {
			var node = flow.nodes[i];
			///@TODO Expand.
		}
	},
	addNodeRow: function(node) {
		var self = this;
		var executingBody = $("#executableBody");
		var tr = document.createElement("tr");
		var tdName = document.createElement("td");
		var tdTimeline = document.createElement("td");
		var tdStart = document.createElement("td");
		var tdEnd = document.createElement("td");
		var tdElapse = document.createElement("td");
		var tdStatus = document.createElement("td");
		var tdDetails = document.createElement("td");
		node.progressbar = tr;
		tr.node = node;
		
		$(tr).append(tdName);
		$(tr).append(tdTimeline);
		$(tr).append(tdStart);
		$(tr).append(tdEnd);
		$(tr).append(tdElapse);
		$(tr).append(tdStatus);
		$(tr).append(tdDetails);
		$(tr).attr("id", node.id + "-row");
		$(tdTimeline).attr("id", node.id + "-timeline");
		$(tdStart).attr("id", node.id + "-start");
		$(tdEnd).attr("id", node.id + "-end");
		$(tdElapse).attr("id", node.id + "-elapse");
		$(tdStatus).attr("id", node.id + "-status");

		var outerProgressBar = document.createElement("div");
		$(outerProgressBar).attr("id", node.id + "-outerprogressbar");
		$(outerProgressBar).addClass("flow-progress");

		var progressBox = document.createElement("div");
		progressBox.job = node.id;
		$(progressBox).attr("id", node.id + "-progressbar");
		$(progressBox).addClass("flow-progress-bar");
		$(outerProgressBar).append(progressBox);
		$(tdTimeline).append(outerProgressBar);
		$(tdTimeline).addClass("timeline");

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
		$(status).attr("id", node.id + "-status-div");
		tdStatus.appendChild(status);

		var logURL = contextURL + "/executor?execid=" + execId + "&job=" + node.nestedId;
		if (node.attempt) {
			logURL += "&attempt=" + node.attempt;
		}

		if (node.type != 'flow' && node.status != 'SKIPPED') {
			var a = document.createElement("a");
			$(a).attr("href", logURL);
			$(a).attr("id", node.id + "-log-link");
			$(a).text("Details");
			$(tdDetails).addClass("details");
			$(tdDetails).append(a);
		}
		executingBody.append(tr);
	}
});

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
azkaban.GraphModel = Backbone.Model.extend({});

var logModel;
azkaban.LogModel = Backbone.Model.extend({});

var updateStatus = function() {
	var requestURL = contextURL + "/executor";
	var oldData = graphModel.get("data");
	var nodeMap = graphModel.get("nodeMap");
	
	var updateTime = oldData.updateTime ? oldData.updateTime : 0;
	var requestData = {
		"execid": execId, 
		"ajax": "fetchexecflowupdate", 
		"lastUpdateTime": updateTime
	};

	graphModel.set({"lastUpdateTime":updateTime})
	
	var successHandler = function(data) {
		console.log("data updated");
		if (data.updateTime) {
			updateTime = data.updateTime;
			
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
		el: $('#jobList'), 
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
		model: flowStatsModel
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
		processFlowData(data);
		graphModel.set({data:data});
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
