$.namespace('azkaban');

function recurseAllAncestors(nodes, disabledMap, id, disable) {
	var node = nodes[id];
	
	if (node.inNodes) {
		for (var key in node.inNodes) {
			disabledMap[key] = disable;
			recurseAllAncestors(nodes, disabledMap, key, disable);
		}
	}
}

function recurseAllDescendents(nodes, disabledMap, id, disable) {
	var node = nodes[id];
	
	if (node.outNodes) {
		for (var key in node.outNodes) {
			disabledMap[key] = disable;
			recurseAllDescendents(nodes, disabledMap, key, disable);
		}
	}
}

var flowExecuteDialogView;
azkaban.FlowExecuteDialogView= Backbone.View.extend({
  events : {
  	"click .simplemodal-close": "hideExecutionOptionPanel",
  	"click .modal-close" : "hideExecutionOptionPanel"
  },
  initialize : function(settings) {
  },
  render: function() {
  },
  show: function(data) {
  	var projectId = data.project;
  	var flowId = data.flow;
  	var jobId = data.job;
  
  	var loadedId = executableGraphModel.get("flowId");
  	if (loadedId != flowId) {
  		this.loadGraph(projectId, flowId);
  	}
  
	if (jobId) {
		this.showExecuteJob(projectId, flowId, jobId, data.withDep);
	}
	else {
		this.showExecuteFlow(projectId, flowId);
	}
  },
  showExecuteFlow: function(projectId, flowId) {
	$("#execute-flow-panel-title").text("Execute Flow " + flowId);
	this.showExecutionOptionPanel();
  },
  showExecuteJob: function(projectId, flowId, jobId, withDep) {
	$("#execute-message").text("Execute the complete flow '" + flowId + "'.");
  },
  showExecutionOptionPanel: function() {
  	$('#modalBackground').show();
  	$('#execute-flow-panel').show();
  },
  hideExecutionOptionPanel: function() {
  	$('#modalBackground').hide();
  	$('#execute-flow-panel').hide();
  },
  loadGraph: function(projectId, flowId) {
  	console.log("Loading flow " + flowId);
  	var managerUrl = contextURL + "/manager";
  	var fetchData = {
  		"ajax" : "fetchflowgraph",
  		"project" : projectId,
  		"flow" : flowId
  	};

  	$.get(
		managerUrl,
		fetchData,
		function(data) {
			if (data.error) {
				alert(data.error);
			}
			else {
				var disabled = data.disabled ? data.disabled : {};
			
				executableGraphModel.set({flowId: data.flowId, data:data, disabled: disabled});
				executableGraphModel.trigger("change:graph");
			}
		},
		"json"
	);
  }
});

var sideMenuDialogView;
azkaban.SideMenuDialogView= Backbone.View.extend({
	events : {
		"click .menuHeader" : "menuClick"
  	},
  	initialize : function(settings) {
  		var children = $(this.el).children();
  		var currentParent;
  		var parents = [];
  		var realChildren = [];
  		for (var i = 0; i < children.length; ++i ) {
  			var child = children[i];
  			if ((i % 2) == 0) {
  				currentParent = child;
  				$(child).addClass("menuHeader");
  				parents.push(child);
  			}
  			else {
  				$(child).addClass("menuContent");
  				$(child).hide();
  				currentParent.child = child;
  				realChildren.push(child);
  			}
  		}
  		
  		this.menuSelect($("#flowOption"));
  		
  		this.parents = parents;
  		this.children = realChildren;
  	},
  	menuClick : function(evt) {
  		this.menuSelect(evt.currentTarget);
  	},
  	menuSelect : function(target) {
  		if ($(target).hasClass("selected")) {
  			return;
  		}
  		
  		$(".sidePanel").each(function() {
  			$(this).hide();
  		});
  		
  		$(".menuHeader").each(function() {
  			$(this.child).slideUp("fast");
  			$(this).removeClass("selected");
  		});
  		
  		$(".sidePanel").each(function() {
  			$(this).hide();
  		});
  		
  		$(target).addClass("selected");
  		$(target.child).slideDown("fast");
  		var panelName = $(target).attr("viewpanel");
  		$("#" + panelName).show();
  	}
});

var handleJobMenuClick = function(action, el, pos) {
	var jobid = el[0].jobid;
	
	var requestURL = contextURL + "/manager?project=" + projectId + "&flow=" + flowName + "&job=" + jobid;
	if (action == "open") {
		window.location.href = requestURL;
	}
	else if(action == "openwindow") {
		window.open(requestURL);
	}
}

var executableGraphModel;
azkaban.GraphModel = Backbone.Model.extend({});

var enableAll = function() {
	disabled = {};
	executableGraphModel.set({disabled: disabled});
	executableGraphModel.trigger("change:disabled");
}

var disableAll = function() {
	var disabled = executableGraphModel.get("disabled");

	var nodes = executableGraphModel.get("nodes");
	for (var key in nodes) {
		disabled[key] = true;
	}

	executableGraphModel.set({disabled: disabled});
	executableGraphModel.trigger("change:disabled");
}

var touchNode = function(jobid, disable) {
	var disabled = executableGraphModel.get("disabled");

	disabled[jobid] = disable;
	executableGraphModel.set({disabled: disabled});
	executableGraphModel.trigger("change:disabled");
}

var touchParents = function(jobid, disable) {
	var disabled = executableGraphModel.get("disabled");
	var nodes = executableGraphModel.get("nodes");
	var inNodes = nodes[jobid].inNodes;

	if (inNodes) {
		for (var key in inNodes) {
		  disabled[key] = disable;
		}
	}
	
	executableGraphModel.set({disabled: disabled});
	executableGraphModel.trigger("change:disabled");
}

var touchChildren = function(jobid, disable) {
	var disabledMap = executableGraphModel.get("disabled");
	var nodes = executableGraphModel.get("nodes");
	var outNodes = nodes[jobid].outNodes;

	if (outNodes) {
		for (var key in outNodes) {
		  disabledMap[key] = disable;
		}
	}
	
	executableGraphModel.set({disabled: disabledMap});
	executableGraphModel.trigger("change:disabled");
}

var touchAncestors = function(jobid, disable) {
	var disabled = executableGraphModel.get("disabled");
	var nodes = executableGraphModel.get("nodes");
	
	recurseAllAncestors(nodes, disabled, jobid, disable);
	
	executableGraphModel.set({disabled: disabled});
	executableGraphModel.trigger("change:disabled");
}

var touchDescendents = function(jobid, disable) {
	var disabled = executableGraphModel.get("disabled");
	var nodes = executableGraphModel.get("nodes");
	
	recurseAllDescendents(nodes, disabled, jobid, disable);
	
	executableGraphModel.set({disabled: disabled});
	executableGraphModel.trigger("change:disabled");
}

var nodeClickCallback = function(event) {
	console.log("Node clicked callback");
	var jobId = event.currentTarget.jobid;
	var flowId = executableGraphModel.get("flowId");
	var requestURL = contextURL + "/manager?project=" + projectId + "&flow=" + flowId + "&job=" + jobId;

	var menu = [	{title: "Open Job in New Window...", callback: function() {window.location.href=requestURL;}},
			{break: 1},
			{title: "Enable", callback: function() {touchNode(jobId, false);}, submenu: [
									{title: "Parents", callback: function(){touchParents(jobId, false);}},
									{title: "Ancestors", callback: function(){touchAncestors(jobId, false);}},
									{title: "Children", callback: function(){touchChildren(jobId, false);}},
									{title: "Descendents", callback: function(){touchDescendents(jobId, false);}},
									{title: "Enable All", callback: function(){enableAll();}}
								]
			},
			{title: "Disable", callback: function() {touchNode(jobId, true)}, submenu: [
									{title: "Parents", callback: function(){touchParents(jobId, true);}},
									{title: "Ancestors", callback: function(){touchAncestors(jobId, true);}},
									{title: "Children", callback: function(){touchChildren(jobId, true);}},
									{title: "Descendents", callback: function(){touchDescendents(jobId, true);}},
									{title: "Disable All", callback: function(){disableAll();}}
								]
			}
	];

	contextMenuView.show(event, menu);
}

var edgeClickCallback = function(event) {
	console.log("Edge clicked callback");
}

var graphClickCallback = function(event) {
	console.log("Graph clicked callback");
	var flowId = executableGraphModel.get("flowId");
	var requestURL = contextURL + "/manager?project=" + projectId + "&flow=" + flowId;
	
	var menu = [	{title: "Open Flow in New Window...", callback: function() {window.location.href=requestURL;}},
		{break: 1},
		{title: "Enable All", callback: function() {enableAll();}},
		{title: "Disable All", callback: function() {disableAll();}},
		{break: 1},
		{title: "Center Graph", callback: function() {executableGraphModel.trigger("resetPanZoom");}}
	];
	
	contextMenuView.show(event, menu);
}

var contextMenuView;
$(function() {
	flowExecuteDialogView = new azkaban.FlowExecuteDialogView({el:$('#execute-flow-panel')});
	executableGraphModel = new azkaban.GraphModel();
	svgGraphView = new azkaban.SvgGraphView({el:$('#svgDivCustom'), model: executableGraphModel, topGId:"topG", graphMargin: 10, rightClick: { "node": nodeClickCallback, "edge": edgeClickCallback, "graph": graphClickCallback }});
	
	sideMenuDialogView = new azkaban.SideMenuDialogView({el:$('#graphOptions')});
	
	var svgGraph = document.getElementById('svgGraph');
	contextMenuView = new azkaban.ContextMenuView({el:$('#contextMenu'), graph: svgGraph});
});
