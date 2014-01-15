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
		var update = this.model.get("update");
		var lastTime = update.endTime == -1 ? (new Date()).getTime() : update.endTime;
		
		if (update.nodes) {
			this.updateJobRow(update.nodes);
		}
		this.updateProgressBar(this.model.get("data"));
	},
	
	updateJobRow: function(nodes) {
		if (!nodes) {
			return;
		}
		
		var executingBody = $("#executableBody");
		nodes.sort(function(a,b) { return a.startTime - b.startTime; });
		
		for (var i = 0; i < nodes.length; ++i) {
			var node = nodes[i].changedNode ? nodes[i].changedNode : nodes[i];
			
			if (node.startTime < 0) {
				continue;
			}
			//var nodeId = node.id.replace(".", "\\\\.");
			var row = node.joblistrow;
			if (!row) {
				this.addNodeRow(node);
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
						attemptBox.job = nodeId;
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
			
			// calculate the progress
			var tr = node.joblistrow;
			
			var progressBar = $(tr).find("> td.timeline > .flow-progress > .main-progress");
			var offsetLeft = 0;
			var minOffset = 0;
			progressBar.attempt = 0;
			
			// Add all the attempts
			if (node.pastAttempts) {
				var logURL = contextURL + "/executor?execid=" + execId + "&job=" + node.id + "&attempt=" +	node.pastAttempts.length;
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
			
			progressBar.css("margin-left", left)
			progressBar.css("width", width);
			progressBar.attr("title", "attempt:" + progressBar.attempt + "	start:" + getHourMinSec(new Date(node.startTime)) + "	end:" + getHourMinSec(new Date(node.endTime)));
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
		node.joblistrow = tr;
		tr.node = node;
		
		$(tr).append(tdName);
		$(tr).append(tdTimeline);
		$(tr).append(tdStart);
		$(tr).append(tdEnd);
		$(tr).append(tdElapse);
		$(tr).append(tdStatus);
		$(tr).append(tdDetails);
		$(tr).addClass("jobListRow");
		$(tdTimeline).addClass("timeline");
		$(tdStart).addClass("startTime");
		$(tdEnd).addClass("endTime");
		$(tdElapse).addClass("elapsedTime");
		$(tdStatus).addClass("statustd");
		$(tdDetails).addClass("details");
		
//		$(tr).attr("id", node.id + "-row");
//		$(tdTimeline).attr("id", node.id + "-timeline");
//		$(tdStart).attr("id", node.id + "-start");
//		$(tdEnd).attr("id", node.id + "-end");
//		$(tdElapse).attr("id", node.id + "-elapse");
//		$(tdStatus).attr("id", node.id + "-status");

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
		executingBody.append(tr);
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

