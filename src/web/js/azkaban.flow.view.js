$.namespace('azkaban');

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
	"QUEUED": "Queued"
};

var handleJobMenuClick = function(action, el, pos) {
	var jobid = el[0].jobid;
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobid;
	if (action == "open") {
		window.location.href = requestURL;
	}
	else if(action == "openwindow") {
		window.open(requestURL);
	}
}

var flowTabView;
azkaban.FlowTabView= Backbone.View.extend({
  events : {
  	"click #graphViewLink" : "handleGraphLinkClick",
  	"click #executionsViewLink" : "handleExecutionLinkClick"
  },
  initialize : function(settings) {
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
  	$("#executionsViewLink").removeClass("selected");
  	$("#graphViewLink").addClass("selected");
  	
  	$("#executionsView").hide();
  	$("#graphView").show();
  },
  handleExecutionLinkClick: function() {
  	$("#graphViewLink").removeClass("selected");
  	$("#executionsViewLink").addClass("selected");
  	
  	 $("#graphView").hide();
  	 $("#executionsView").show();
  	 executionModel.trigger("change:view");
  }
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
		var numPages = Math.ceil(total/pageSize);
		
		this.model.set({"numPages": numPages});
		var page = this.model.get("page");
		
		//Start it off
		$("#pageSelection .selected").removeClass("selected");
		
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
		if (page < 3) {
			selectionPosition = page;
		}
		else if (page > numPages - 2) {
			selectionPosition = 5 - (numPages - page) - 1;
		}
		else {
			selectionPosition = 3;
		}

		$("#page"+selectionPosition).addClass("selected");
		$("#page"+selectionPosition)[0].page = page;
		var selecta = $("#page" + selectionPosition + " a");
		selecta.text(page);
		selecta.attr("href", "#page" + page);

		for (var j = 1, tpage = page - selectionPosition + 1; j < selectionPosition; ++j, ++tpage) {
			$("#page" + j)[0].page = tpage;
			var a = $("#page" + i + " a");
			a.text(tpage);
			a.attr("href", "#page" + tpage);
		}

		for (var i = selectionPosition + 1, tpage = page + 1; i <= numPages; ++i, ++tpage) {
			$("#page" + i)[0].page = tpage;
			var a = $("#page" + i + " a");
			a.text(tpage);
			a.attr("href", "#page" + tpage);
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
		$.get(
			requestURL,
			{"project": projectName, "flow":flowId, "ajax": "fetchFlowExecutions", "start":page * pageSize, "length": pageSize},
			function(data) {
				model.set({"executions": data.executions, "total": data.total});
				model.trigger("render");
			},
			"json"
		);
		
	}
});

var graphModel;
azkaban.GraphModel = Backbone.Model.extend({});

var executionModel;
azkaban.ExecutionModel = Backbone.Model.extend({});
var mainSvgGraphView;

$(function() {
	var selected;
	// Execution model has to be created before the window switches the tabs.
	executionModel = new azkaban.ExecutionModel();
	executionsView = new azkaban.ExecutionsView({el:$('#executionsView'), model: executionModel});
	
	flowTabView = new azkaban.FlowTabView({el:$( '#headertabs'), selectedView: selected });

	graphModel = new azkaban.GraphModel();
	mainSvgGraphView = new azkaban.SvgGraphView({el:$('#svgDiv'), model: graphModel, rightClick:  { "node": nodeClickCallback, "edge": edgeClickCallback, "graph": graphClickCallback }});
	jobsListView = new azkaban.JobListView({el:$('#jobList'), model: graphModel, contextMenuCallback: jobClickCallback});
	
	setTimeout(
		function() { 
			processFlowData(jsonFlowObject);
			graphModel.set({data:jsonFlowObject});
    		graphModel.trigger("change:graph");
		}, 
	10);


//
//	$.get(
//	      requestURL,
//	      {},
//	      function(data) {
//	    	  processFlowData(data);
//	    	  graphModel.set({data:data});
//	          graphModel.trigger("change:graph");
//	          
//	          // Handle the hash changes here so the graph finishes rendering first.
//	          if (window.location.hash) {
//				var hash = window.location.hash;
//				
//				if (hash == "#executions") {
//					flowTabView.handleExecutionLinkClick();
//				}
//				else if (hash == "#graph") {
//					// Redundant, but we may want to change the default. 
//					selected = "graph";
//				}
//				else {
//					if ("#page" == hash.substring(0, "#page".length)) {
//						var page = hash.substring("#page".length, hash.length);
//						console.log("page " + page);
//						flowTabView.handleExecutionLinkClick();
//						executionModel.set({"page": parseInt(page)});
//					}
//					else {
//						selected = "graph";
//					}
//				}
//			}
//	      },
//	      "json"
//	    );

});
