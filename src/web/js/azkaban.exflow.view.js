$.namespace('azkaban');

var statusList = ["FAILED", "FAILED_FINISHING", "SUCCEEDED", "RUNNING", "WAITING", "KILLED", "DISABLED", "READY", "UNKNOWN", "PAUSED"];
var statusStringMap = {
	"FAILED": "Failed",
	"SUCCEEDED": "Success",
	"FAILED_FINISHING": "Running w/Failure",
	"RUNNING": "Running",
	"WAITING": "Waiting",
	"KILLED": "Killed",
	"DISABLED": "Disabled",
	"READY": "Ready",
	"UNKNOWN": "Unknown",
	"PAUSED": "Paused"
};

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

function hasClass(el, name) 
{
	var classes = el.getAttribute("class");
	if (classes == null) {
		return false;
	}
   return new RegExp('(\\s|^)'+name+'(\\s|$)').test(classes);
}

function addClass(el, name)
{
   if (!hasClass(el, name)) { 
   		var classes = el.getAttribute("class");
   		classes += classes ? ' ' + name : '' +name;
   		el.setAttribute("class", classes);
   }
}

function removeClass(el, name)
{
   if (hasClass(el, name)) {
      var classes = el.getAttribute("class");
      el.setAttribute("class", classes.replace(new RegExp('(\\s|^)'+name+'(\\s|$)'),' ').replace(/^\s+|\s+$/g, ''));
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
  	"click #restartbtn" : "handleRestartClick",
  	"click #pausebtn" : "handlePauseClick",
  	"click #resumebtn" : "handleResumeClick",
  },
  initialize : function(settings) {
  	$("#cancelbtn").hide();
  	$("#restartbtn").hide();
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
  	$("#restartbtn").hide();
  	$("#pausebtn").hide();
  	$("#resumebtn").hide();

  	if(data.status=="SUCCEEDED") {
  	  	$("#restartbtn").show();
  	}
  	else if (data.status=="FAILED") {
  		$("#restartbtn").show();
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
  		$("#restartbtn").show();
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
azkaban.JobListView = Backbone.View.extend({
	events: {
		"keyup input": "filterJobs",
		"click li": "handleJobClick",
		"click #resetPanZoomBtn" : "handleResetPanZoom"
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.handleSelectionChange, this);
		this.model.bind('change:graph', this.render, this);
		this.model.bind('change:update', this.handleStatusUpdate, this);
	},
	filterJobs: function(self) {
		var filter = $("#filter").val();
		
		if (filter && filter.trim() != "") {
			filter = filter.trim();
			
			if (filter == "") {
				if (this.filter) {
					$("#jobs").children().each(
						function(){
							var a = $(this).find("a");
        					$(a).html(this.jobid);
        					$(this).show();
						}
					);
				}
				
				this.filter = null;
				return;
			}
		}
		else {
			if (this.filter) {
				$("#jobs").children().each(
					function(){
						var a = $(this).find("a");
    					$(a).html(this.jobid);
    					$(this).show();
					}
				);
			}
				
			this.filter = null;
			return;
		}
		
		$("#jobs").children().each(
			function(){
        		var jobid = this.jobid;
        		var index = jobid.indexOf(filter);
        		if (index != -1) {
        			var a = $(this).find("a");
        			
        			var endIndex = index + filter.length;
        			var newHTML = jobid.substring(0, index) + "<span>" + jobid.substring(index, endIndex) + "</span>" + jobid.substring(endIndex, jobid.length);
        			
        			$(a).html(newHTML);
        			$(this).show();
        		}
        		else {
        			$(this).hide();
        		}
    	});
    	
    	this.filter = filter;
	},
	render: function(self) {
		var data = this.model.get("data");
		var nodes = data.nodes;
		var edges = data.edges;
		
		this.listNodes = {}; 
		if (nodes.length == 0) {
			console.log("No results");
			return;
		};
	
		var nodeArray = nodes.slice(0);
		nodeArray.sort(function(a,b){ 
			var diff = a.y - b.y;
			if (diff == 0) {
				return a.x - b.x;
			}
			else {
				return diff;
			}
		});
		
		var ul = document.createElement("ul");
		$(ul).attr("id", "jobs");
		for (var i = 0; i < nodeArray.length; ++i) {
			var li = document.createElement("li");
			
			var iconDiv = document.createElement("div");
			$(iconDiv).addClass("icon");
			li.appendChild(iconDiv);
			
			var a = document.createElement("a");
			$(a).text(nodeArray[i].id);
			li.appendChild(a);
			ul.appendChild(li);
			li.jobid=nodeArray[i].id;
			
			$(li).contextMenu({
					menu: 'jobMenu'
				},
				handleJobMenuClick
			);
			
			this.listNodes[nodeArray[i].id] = li;
		}
		
		$("#list").append(ul);
		this.assignInitialStatus(self);
	},
	handleJobClick : function(evt) {
		var jobid = evt.currentTarget.jobid;
		if(!evt.currentTarget.jobid) {
			return;
		}
		
		if (this.model.has("selected")) {
			var selected = this.model.get("selected");
			if (selected == jobid) {
				this.model.unset("selected");
			}
			else {
				this.model.set({"selected": jobid});
			}
		}
		else {
			this.model.set({"selected": jobid});
		}
	},
	handleStatusUpdate: function(evt) {
		var updateData = this.model.get("update");
		for (var i = 0; i < updateData.nodes.length; ++i) {
			var updateNode = updateData.nodes[i];
			$(this.listNodes[updateNode.id]).addClass(updateNode.status);
		}
	},
	assignInitialStatus: function(evt) {
		var data = this.model.get("data");
		for (var i = 0; i < data.nodes.length; ++i) {
			var updateNode = data.nodes[i];
			
			$(this.listNodes[updateNode.id]).addClass(updateNode.status);
		}
	},
	handleSelectionChange: function(evt) {
		if (!this.model.hasChanged("selected")) {
			return;
		}
		
		var previous = this.model.previous("selected");
		var current = this.model.get("selected");
		
		if (previous) {
			$(this.listNodes[previous]).removeClass("selected");
		}
		
		if (current) {
			$(this.listNodes[current]).addClass("selected");
		}
	},
	handleResetPanZoom: function(evt) {
		this.model.trigger("resetPanZoom");
	}
});

var svgGraphView;
azkaban.SvgGraphView = Backbone.View.extend({
	events: {
		"click g" : "clickGraph"
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.changeSelected, this);
		this.model.bind('change:graph', this.render, this);
		this.model.bind('resetPanZoom', this.resetPanZoom, this);
		this.model.bind('change:update', this.handleStatusUpdate, this);
		
		this.svgns = "http://www.w3.org/2000/svg";
		this.xlinksn = "http://www.w3.org/1999/xlink";
		
		var graphDiv = this.el[0];
		var svg = $('#svgGraph')[0];
		this.svgGraph = svg;
		
		var gNode = document.createElementNS(this.svgns, 'g');
		gNode.setAttribute("id", "group");
		svg.appendChild(gNode);
		this.mainG = gNode;

		$(svg).svgNavigate();
	},
	initializeDefs: function(self) {
		var def = document.createElementNS(svgns, 'defs');
		def.setAttributeNS(null, "id", "buttonDefs");

		// ArrowHead
		var arrowHeadMarker = document.createElementNS(svgns, 'marker');
		arrowHeadMarker.setAttribute("id", "triangle");
		arrowHeadMarker.setAttribute("viewBox", "0 0 10 10");
		arrowHeadMarker.setAttribute("refX", "5");
		arrowHeadMarker.setAttribute("refY", "5");
		arrowHeadMarker.setAttribute("markerUnits", "strokeWidth");
		arrowHeadMarker.setAttribute("markerWidth", "4");
		arrowHeadMarker.setAttribute("markerHeight", "3");
		arrowHeadMarker.setAttribute("orient", "auto");
		var path = document.createElementNS(svgns, 'polyline');
		arrowHeadMarker.appendChild(path);
		path.setAttribute("points", "0,0 10,5 0,10 1,5");

		def.appendChild(arrowHeadMarker);
		
		this.svgGraph.appendChild(def);
	},
	render: function(self) {
		console.log("graph render");

		var data = this.model.get("data");
		var nodes = data.nodes;
		var edges = data.edges;
		if (nodes.length == 0) {
			console.log("No results");
			return;
		};
	
		// layout
		layoutGraph(nodes, edges);
		
		var bounds = {};
		this.nodes = {};
		for (var i = 0; i < nodes.length; ++i) {
			this.nodes[nodes[i].id] = nodes[i];
		}
		
		for (var i = 0; i < edges.length; ++i) {
			this.drawEdge(this, edges[i]);
		}
		
		for (var i = 0; i < nodes.length; ++i) {
			this.drawNode(this, nodes[i], bounds);
		}
		
		bounds.minX = bounds.minX ? bounds.minX - 200 : -200;
		bounds.minY = bounds.minY ? bounds.minY - 200 : -200;
		bounds.maxX = bounds.maxX ? bounds.maxX + 200 : 200;
		bounds.maxY = bounds.maxY ? bounds.maxY + 200 : 200;
		
		this.assignInitialStatus(self);
		this.graphBounds = bounds;
		this.resetPanZoom();
	},
	assignInitialStatus: function(evt) {
		var data = this.model.get("data");
		for (var i = 0; i < data.nodes.length; ++i) {
			var updateNode = data.nodes[i];
			var g = document.getElementById(updateNode.id);
			addClass(g, updateNode.status);
		}
	},
	changeSelected: function(self) {
		console.log("change selected");
		var selected = this.model.get("selected");
		var previous = this.model.previous("selected");
		
		if (previous) {
			// Unset previous
			var g = document.getElementById(previous);
			removeClass(g, "selected");
		}
		
		if (selected) {
			var g = document.getElementById(selected);
			var node = this.nodes[selected];
			
			addClass(g, "selected");
			
			var offset = 200;
			var widthHeight = offset*2;
			var x = node.x - offset;
			var y = node.y - offset;
			
			$("#svgGraph").svgNavigate("transformToBox", {x: x, y: y, width: widthHeight, height: widthHeight});
		}
	},
	handleStatusUpdate: function(evt) {
		var updateData = this.model.get("update");
		for (var i = 0; i < updateData.nodes.length; ++i) {
			var updateNode = updateData.nodes[i];
			var g = document.getElementById(updateNode.id);
			
			for (var j = 0; j < statusList.length; ++j) {
				var status = statusList[j];
				removeClass(g, status);
			}
			
			addClass(g, updateNode.status);
		}
	},
	clickGraph: function(self) {
		console.log("click");
		if (self.currentTarget.jobid) {
			this.model.set({"selected": self.currentTarget.jobid});
		}
	},
	drawEdge: function(self, edge) {
		var svg = self.svgGraph;
		var svgns = self.svgns;
		
		var startNode = this.nodes[edge.from];
		var endNode = this.nodes[edge.target];
		
		if (edge.guides) {
			var pointString = "" + startNode.x + "," + startNode.y + " ";

			for (var i = 0; i < edge.guides.length; ++i ) {
				edgeGuidePoint = edge.guides[i];
				pointString += edgeGuidePoint.x + "," + edgeGuidePoint.y + " ";
			}
			
			pointString += endNode.x + "," + endNode.y;
			var polyLine = document.createElementNS(svgns, "polyline");
			polyLine.setAttributeNS(null, "class", "edge");
			polyLine.setAttributeNS(null, "points", pointString);
			polyLine.setAttributeNS(null, "style", "fill:none;");
			self.mainG.appendChild(polyLine);
		}
		else { 
			var line = document.createElementNS(svgns, 'line');
			line.setAttributeNS(null, "class", "edge");
			line.setAttributeNS(null, "x1", startNode.x);
			line.setAttributeNS(null, "y1", startNode.y);
			line.setAttributeNS(null, "x2", endNode.x);
			line.setAttributeNS(null, "y2", endNode.y);
			
			self.mainG.appendChild(line);
		}
	},
	drawNode: function(self, node, bounds) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var xOffset = 10;
		var yOffset = 10;

		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttributeNS(null, "class", "jobnode");
		nodeG.setAttributeNS(null, "id", node.id);
		nodeG.setAttributeNS(null, "font-family", "helvetica");
		nodeG.setAttributeNS(null, "transform", "translate(" + node.x + "," + node.y + ")");
		
		var innerG = document.createElementNS(svgns, "g");
		innerG.setAttributeNS(null, "transform", "translate(-10,-10)");
		
		var circle = document.createElementNS(svgns, 'circle');
		circle.setAttributeNS(null, "cy", 10);
		circle.setAttributeNS(null, "cx", 10);
		circle.setAttributeNS(null, "r", 12);
		circle.setAttributeNS(null, "style", "width:inherit;stroke-opacity:1");
		
		
		var text = document.createElementNS(svgns, 'text');
		var textLabel = document.createTextNode(node.label);
		text.appendChild(textLabel);
		text.setAttributeNS(null, "x", 4);
		text.setAttributeNS(null, "y", 15);
		text.setAttributeNS(null, "height", 10); 
				
		this.addBounds(bounds, {minX:node.x - xOffset, minY: node.y - yOffset, maxX: node.x + xOffset, maxY: node.y + yOffset});
		
		var backRect = document.createElementNS(svgns, 'rect');
		backRect.setAttributeNS(null, "x", 0);
		backRect.setAttributeNS(null, "y", 2);
		backRect.setAttributeNS(null, "class", "backboard");
		backRect.setAttributeNS(null, "width", 10);
		backRect.setAttributeNS(null, "height", 15);
		
		innerG.appendChild(circle);
		innerG.appendChild(backRect);
		innerG.appendChild(text);
		innerG.jobid = node.id;

		nodeG.appendChild(innerG);
		self.mainG.appendChild(nodeG);

		// Need to get text width after attaching to SVG.
		var computeText = text.getComputedTextLength();
		var halfWidth = computeText/2;
		text.setAttributeNS(null, "x", -halfWidth + 10);
		backRect.setAttributeNS(null, "x", -halfWidth);
		backRect.setAttributeNS(null, "width", computeText + 20);

		nodeG.setAttributeNS(null, "class", "node");
		nodeG.jobid=node.id;
		$(nodeG).contextMenu({
				menu: 'jobMenu'
			},
			handleJobMenuClick
		);
	},
	addBounds: function(toBounds, addBounds) {
		toBounds.minX = toBounds.minX ? Math.min(toBounds.minX, addBounds.minX) : addBounds.minX;
		toBounds.minY = toBounds.minY ? Math.min(toBounds.minY, addBounds.minY) : addBounds.minY;
		toBounds.maxX = toBounds.maxX ? Math.max(toBounds.maxX, addBounds.maxX) : addBounds.maxX;
		toBounds.maxY = toBounds.maxY ? Math.max(toBounds.maxY, addBounds.maxY) : addBounds.maxY;
	},
	resetPanZoom : function(self) {
		var bounds = this.graphBounds;
		$("#svgGraph").svgNavigate("transformToBox", {x: bounds.minX, y: bounds.minY, width: (bounds.maxX - bounds.minX), height: (bounds.maxY - bounds.minY) });
	}
});

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
					$("#" + node.id + "-elapse").text("0 sec");
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
			var width = Math.max((node.endTime - node.startTime)*factor, 1);
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

		var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowName + "&job=" + node.id;
		var a = document.createElement("a");
		$(a).attr("href", requestURL);
		$(a).text(node.id);
		$(tdName).append(a);

		var status = document.createElement("div");
		$(status).addClass("status");
		$(status).attr("id", node.id + "-status-div");
		tdStatus.appendChild(status);

		var logURL = contextURL + "/executor?execid=" + execId + "&flow=" + flowName + "&job=" + node.id;
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
		this.model.set({"current": 0});
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var current = this.model.get("current");
		var requestURL = contextURL + "/executor"; 
		var model = this.model;
		ajaxLogsCall(
			requestURL,
			{"execid": execId, "ajax":"fetchExecFlowLogs", "current": current, "max": 100000},
			function(data) {
	          console.log("fetchLogs");
	          if (data.error) {
	          	showDialog("Error", data.error);
	          }
	          else {
	          	var log = $("#logSection").text();
	          	if (!log) {
	          		log = data.log;
	          	}
	          	else {
	          		log += data.log;
	          	}
	          	
	          	current = data.current;
	          	$("#logSection").text(log);
	          	model.set({"current": current, "log": log});
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
	          updateTime = Math.max(updateTime, data.submitTime);
	          updateTime = Math.max(updateTime, data.startTime);
	          updateTime = Math.max(updateTime, data.endTime);
	          oldData.submitTime = data.submitTime;
	          oldData.startTime = data.startTime;
	          oldData.endTime = data.endTime;
	          oldData.status = data.status;
	          
	          for (var i = 0; i < data.nodes.length; ++i) {
	          	var node = data.nodes[i];
	          	updateTime = Math.max(updateTime, node.startTime);
	          	updateTime = Math.max(updateTime, node.endTime);
	          	var oldNode = nodeMap[node.id];
	          	oldNode.startTime = node.startTime;
	          	oldNode.endTime = node.endTime;
	          	oldNode.status = node.status;
	          }

	          graphModel.set({"update": data});
	      });
}

var updateTime = -1;
var updaterFunction = function() {
	var oldData = graphModel.get("data");
	var keepRunning = oldData.status != "SUCCEEDED" && oldData.status != "FAILED" && oldData.status != "KILLED";

	if (keepRunning) {
		updateStatus();

		var data = graphModel.get("data");
		if (data.status == "UNKNOWN" || data.status == "WAITING") {
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
	svgGraphView = new azkaban.SvgGraphView({el:$('#svgDiv'), model: graphModel});
	jobsListView = new azkaban.JobListView({el:$('#jobList'), model: graphModel});
	statusView = new azkaban.StatusView({el:$('#flow-status'), model: graphModel});
	flowLogView = new azkaban.FlowLogView({el:$('#flowLogView'), model: logModel});
	executionListView = new azkaban.ExecutionListView({el: $('#jobListView'), model:graphModel});
	var requestURL = contextURL + "/executor";

	ajaxCall(
	      requestURL,
	      {"execid": execId, "ajax":"fetchexecflow"},
	      function(data) {
	          console.log("data fetched");
	          graphModel.set({data: data});
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
