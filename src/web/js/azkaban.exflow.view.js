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
azkaban.StatusView= Backbone.View.extend({
	initialize : function(settings) {
		this.model.bind('change:graph', this.render, this);
		this.model.bind('change:update', this.statusUpdate, this);
	},
	render : function(evt) {
		var data = this.model.get("data");
		
		var user = data.submitUser;
		$("#submitUser").text(user);
		
		this.statusUpdate(evt);
	},
	statusUpdate : function(evt) {
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
		
		if (startTime == -1) {
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
		
		if (endTime == -1) {
			$("#endTime").text("-");
		}
		else {
			var date = new Date(endTime);
			$("#endTime").text(getDateFormat(date));
		}
	}
});

var flowTabView;
azkaban.FlowTabView= Backbone.View.extend({
  events : {
  	"click #graphViewLink" : "handleGraphLinkClick",
  	"click #jobslistViewLink" : "handleJobslistLinkClick",
  	"click #flowLogViewLink" : "handleLogLinkClick",
  	"click #cancelbtn" : "handleCancelClick",
  	"click #executebtn" : "handleRestartClick",
  	"click #pausebtn" : "handlePauseClick",
  	"click #resumebtn" : "handleResumeClick",
  },
  initialize : function(settings) {
  	$("#cancelbtn").hide();
  	$("#executebtn").hide();
  	$("#pausebtn").hide();
  	$("#resumebtn").hide();
  
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
  	$("#jobslistViewLink").removeClass("selected");
  	$("#graphViewLink").addClass("selected");
  	$("#flowLogViewLink").removeClass("selected");
  	
  	$("#jobListView").hide();
  	$("#graphView").show();
  	$("#flowLogView").hide();
  },
  handleJobslistLinkClick: function() {
  	$("#graphViewLink").removeClass("selected");
  	$("#jobslistViewLink").addClass("selected");
  	$("#flowLogViewLink").removeClass("selected");
  	
  	$("#graphView").hide();
  	$("#jobListView").show();
  	$("#flowLogView").hide();
  },
  handleLogLinkClick: function() {
  	$("#graphViewLink").removeClass("selected");
  	$("#jobslistViewLink").removeClass("selected");
  	$("#flowLogViewLink").addClass("selected");
  	
  	$("#graphView").hide();
  	$("#jobListView").hide();
  	$("#flowLogView").show();
  },
  handleFlowStatusChange: function() {
  	var data = this.model.get("data");
  	$("#cancelbtn").hide();
  	$("#executebtn").hide();
  	$("#pausebtn").hide();
  	$("#resumebtn").hide();

  	if(data.status=="SUCCEEDED") {
  	  	$("#executebtn").show();
  	}
  	else if (data.status=="FAILED") {
  		$("#executebtn").show();
  	}
  	else if (data.status=="FAILED_FINISHING") {
  		$("#cancelbtn").show();
  	}
  	else if (data.status=="RUNNING") {
  		$("#cancelbtn").show();
  		$("#pausebtn").show();
  	}
  	else if (data.status=="PAUSED") {
  		$("#cancelbtn").show();
  		$("#resumebtn").show();
  	}
  	else if (data.status=="WAITING") {
  		$("#cancelbtn").show();
  	}
  	else if (data.status=="KILLED") {
  		$("#executebtn").show();
  	}
  },
  handleCancelClick : function(evt) {
    var requestURL = contextURL + "/executor";
	ajaxCall(
		requestURL,
		{"execid": execId, "ajax":"cancelFlow"},
		function(data) {
          console.log("cancel clicked");
          if (data.error) {
          	showDialog("Error", data.error);
          }
          else {
            showDialog("Cancelled", "Flow has been cancelled.");

            setTimeout(function() {updateStatus();}, 1100);
          }
      	}
      );
  },
  handleRestartClick : function(evt) {
  	  	executeFlowView.show();
  },
  handlePauseClick : function(evt) {
  	  var requestURL = contextURL + "/executor";
		ajaxCall(
	      requestURL,
	      {"execid": execId, "ajax":"pauseFlow"},
	      function(data) {
	          console.log("pause clicked");
	          if (data.error) {
	          	showDialog("Error", data.error);
	          }
	          else {
	            showDialog("Paused", "Flow has been paused.");
	            
            	setTimeout(function() {updateStatus();}, 1100);
	          }
	      }
      );
  },
  handleResumeClick : function(evt) {
     var requestURL = contextURL + "/executor";
     ajaxCall(
          requestURL,
	      {"execid": execId, "ajax":"resumeFlow"},
	      function(data) {
	          console.log("pause clicked");
	          if (data.error) {
	          	showDialog("Error", data.error);
	          }
	          else {
	          	showDialog("Resumed", "Flow has been resumed.");
            	setTimeout(function() {updateStatus();}, 1100);
	          }
	      }
	  );
  }
});

var showDialog = function(title, message) {
  $('#messageTitle').text(title);

  $('#messageBox').text(message);

  $('#messageDialog').modal({
      closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
      position: ["20%",],
      containerId: 'confirm-container',
      containerCss: {
        'height': '220px',
        'width': '565px'
      },
      onShow: function (dialog) {
      }
    });
}

var jobListView;
var svgGraphView;

var executionListView;
azkaban.ExecutionListView = Backbone.View.extend({
	events: {
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
	updateJobs: function(evt) {
		var data = this.model.get("update");
		var lastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
		
		this.updateJobRow(data.nodes);
		this.updateProgressBar(this.model.get("data"));
	},
	updateJobRow: function(nodes) {
		var executingBody = $("#executableBody");
		nodes.sort(function(a,b) { return a.startTime - b.startTime; });
		
		for (var i = 0; i < nodes.length; ++i) {
			var node = nodes[i];
			if (node.startTime > -1) {
				var row = document.getElementById(node.id + "-row");
				if (!row) {
					this.addNodeRow(node);
				}
				
				var div = $("#" + node.id + "-status-div");
				div.text(statusStringMap[node.status]);
				$(div).attr("class", "status " + node.status);
				
				var startdate = new Date(node.startTime);
				$("#" + node.id + "-start").text(getDateFormat(startdate));
				
				var endTime = node.endTime;
				if (node.endTime == -1) {
					$("#" + node.id + "-end").text("-");
					endTime = node.startTime + 1;
				}
				else {
					var enddate = new Date(node.endTime);
					$("#" + node.id + "-end").text(getDateFormat(enddate));
				}
				
				var progressBar = $("#" + node.id + "-progressbar");
				for (var j = 0; j < statusList.length; ++j) {
					var status = statusList[j];
					progressBar.removeClass(status);
				}
				progressBar.addClass(node.status);

				if (node.endTime == -1) {
//					$("#" + node.id + "-elapse").text("0 sec");
					$("#" + node.id + "-elapse").text(getDuration(node.startTime, (new Date()).getTime()));					
				}
				else {
					$("#" + node.id + "-elapse").text(getDuration(node.startTime, node.endTime));
				}
			}
		}
	},
	updateProgressBar: function(data) {
		if(data.startTime == -1) {
			return;
		}
		
		var flowLastTime = data.endTime == -1 ? (new Date()).getTime() : data.endTime;
		var flowStartTime = data.startTime;
		
		var outerWidth = $(".outerProgress").css("width");
		if (outerWidth.substring(outerWidth.length - 2, outerWidth.length) == "px") {
			outerWidth = outerWidth.substring(0, outerWidth.length - 2);
		}
		outerWidth = parseInt(outerWidth);
		
		var nodes = data.nodes;
		for (var i = 0; i < nodes.length; ++i) {
			var node = nodes[i];
		
			// calculate the progress
			var diff = flowLastTime - flowStartTime;
			
			var factor = outerWidth/diff;
			var left = Math.max((node.startTime-flowStartTime)*factor, 0);
			var nodeLastTime = node.endTime == -1 ? (new Date()).getTime() : node.endTime;
			var width = Math.max((nodeLastTime - node.startTime)*factor, 3);
			width = Math.min(width, outerWidth);
			
			$("#" + node.id + "-progressbar").css("margin-left", left)
			$("#" + node.id + "-progressbar").css("width", width);
		}
	},
	addNodeRow: function(node) {
		var executingBody = $("#executableBody");
		var tr = document.createElement("tr");
		var tdName = document.createElement("td");
		var tdTimeline = document.createElement("td");
		var tdStart = document.createElement("td");
		var tdEnd = document.createElement("td");
		var tdElapse = document.createElement("td");
		var tdStatus = document.createElement("td");
		var tdLog = document.createElement("td");
		
		$(tr).append(tdName);
		$(tr).append(tdTimeline);
		$(tr).append(tdStart);
		$(tr).append(tdEnd);
		$(tr).append(tdElapse);
		$(tr).append(tdStatus);
		$(tr).append(tdLog);
		$(tr).attr("id", node.id + "-row");
		$(tdTimeline).attr("id", node.id + "-timeline");
		$(tdStart).attr("id", node.id + "-start");
		$(tdEnd).attr("id", node.id + "-end");
		$(tdElapse).attr("id", node.id + "-elapse");
		$(tdStatus).attr("id", node.id + "-status");

		var outerProgressBar = document.createElement("div");
		$(outerProgressBar).addClass("outerProgress");

		var progressBox = document.createElement("div");
		$(progressBox).attr("id", node.id + "-progressbar");
		$(progressBox).addClass("progressBox");
		$(outerProgressBar).append(progressBox);
		$(tdTimeline).append(outerProgressBar);
		$(tdTimeline).addClass("timeline");

		var requestURL = contextURL + "/manager?project=" + projectName + "&job=" + node.id + "&history";
		var a = document.createElement("a");
		$(a).attr("href", requestURL);
		$(a).text(node.id);
		$(tdName).append(a);

		var status = document.createElement("div");
		$(status).addClass("status");
		$(status).attr("id", node.id + "-status-div");
		tdStatus.appendChild(status);

		var logURL = contextURL + "/executor?execid=" + execId + "&job=" + node.id;
		var a = document.createElement("a");
		$(a).attr("href", logURL);
		$(a).text("Log");
		$(tdLog).addClass("logLink");
		$(tdLog).append(a);

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
		$.get(
			requestURL,
			{"execid": execId, "ajax":"fetchExecFlowLogs", "offset": offset, "length": 50000},
			function(data) {
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
	    );
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
	
	ajaxCall(
	      requestURL,
	      {"execid": execId, "ajax":"fetchexecflowupdate", "lastUpdateTime": updateTime},
	      function(data) {
	          console.log("data updated");
	          updateTime = data.updateTime;
	          oldData.submitTime = data.submitTime;
	          oldData.startTime = data.startTime;
	          oldData.endTime = data.endTime;
	          oldData.status = data.status;
	          
	          for (var i = 0; i < data.nodes.length; ++i) {
	          	var node = data.nodes[i];
	          	var oldNode = nodeMap[node.id];
	          	oldNode.startTime = node.startTime;
	          	oldNode.updateTime = node.updateTime;
	          	oldNode.endTime = node.endTime;
	          	oldNode.status = node.status;
	          }

	          graphModel.set({"update": data});
	          graphModel.trigger("change:update");
	      });
}

var updateTime = -1;
var updaterFunction = function() {
	var oldData = graphModel.get("data");
	var keepRunning = oldData.status != "SUCCEEDED" && oldData.status != "FAILED" && oldData.status != "KILLED";

	if (keepRunning) {
		updateStatus();

		var data = graphModel.get("data");
		if (data.status == "UNKNOWN" || data.status == "WAITING" || data.status == "PREPARING") {
			setTimeout(function() {updaterFunction();}, 1000);
		}
		else if (data.status != "SUCCEEDED" && data.status != "FAILED" ) {
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
	var keepRunning = oldData.status != "SUCCEEDED" && oldData.status != "FAILED" && oldData.status != "KILLED";
	if (keepRunning) {
		// update every 30 seconds for the logs until finished
		flowLogView.handleUpdate();
		setTimeout(function() {logUpdaterFunction();}, 30000);
	}
	else {
		flowLogView.handleUpdate();
	}
}

$(function() {
	var selected;

	graphModel = new azkaban.GraphModel();
	logModel = new azkaban.LogModel();
	flowTabView = new azkaban.FlowTabView({el:$( '#headertabs'), model: graphModel});
	svgGraphView = new azkaban.SvgGraphView({el:$('#svgDiv'), model: graphModel, rightClick: {id: 'jobMenu', callback: handleJobMenuClick}});
	jobsListView = new azkaban.JobListView({el:$('#jobList'), model: graphModel, rightClick: {id: 'jobMenu', callback: handleJobMenuClick}});
	statusView = new azkaban.StatusView({el:$('#flow-status'), model: graphModel});
	flowLogView = new azkaban.FlowLogView({el:$('#flowLogView'), model: logModel});
	executeFlowView = new azkaban.ExecuteFlowView({el:$('#executing-options'), model: graphModel});
	executionListView = new azkaban.ExecutionListView({el: $('#jobListView'), model:graphModel});
	var requestURL = contextURL + "/executor";

	ajaxCall(
	      requestURL,
	      {"execid": execId, "ajax":"fetchexecflow"},
	      function(data) {
	          console.log("data fetched");
	          graphModel.set({data: data});
	          graphModel.set({disabled: {}});
	          graphModel.trigger("change:graph");
	          
	          updateTime = Math.max(updateTime, data.submitTime);
	          updateTime = Math.max(updateTime, data.startTime);
	          updateTime = Math.max(updateTime, data.endTime);
	          
	          var nodeMap = {};
	          for (var i = 0; i < data.nodes.length; ++i) {
	             var node = data.nodes[i];
	             nodeMap[node.id] = node;
	             updateTime = Math.max(updateTime, node.startTime);
	             updateTime = Math.max(updateTime, node.endTime);
	          }
	          
	          graphModel.set({nodeMap: nodeMap});
	          
	          if (window.location.hash) {
					var hash = window.location.hash;
					if (hash == "#jobslist") {
						flowTabView.handleJobslistLinkClick();
					}
					else if (hash == "#log") {
						flowTabView.handleLogLinkClick();
					}
			 }
	          
	      	 updaterFunction();
	      	 logUpdaterFunction();
	      }
	    );
});
