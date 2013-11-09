var extendedViewPanels = {};
var extendedDataModels = {};
var openJobDisplayCallback = function(nodeId, flowId, evt) {
	console.log("Open up data");
	
	var nodeInfoPanelID = flowId + ":" + nodeId + "-info";
	if ($("#" + nodeInfoPanelID).length) {
		$("#flowInfoBase").before(cloneStuff);
		extendedViewPanels[nodeInfoPanelID].showExtendedView(evt);
		return;
	}
	
	var cloneStuff = $("#flowInfoBase").clone();
	$(cloneStuff).attr("id", nodeInfoPanelID);
	
	$("#flowInfoBase").before(cloneStuff);
	var requestURL = contextURL + "/manager";
	var successHandler = function(data) {
		var graphModel = new azkaban.GraphModel();
		graphModel.set({
			id: data.id, 
			flow: data.flowData, 
			type: data.type, 
			props: data.props
		});

		var flowData = data.flowData;
		if (flowData) {
			createModelFromAjaxCall(flowData, graphModel);
		}
		
		var backboneView = new azkaban.FlowExtendedViewPanel({
			el: cloneStuff, 
			model: graphModel
		});
		extendedViewPanels[nodeInfoPanelID] = backboneView;
		extendedDataModels[nodeInfoPanelID] = graphModel;
		backboneView.showExtendedView(evt);
  };

  var request = {
		"project": projectName, 
		"ajax": "fetchflownodedata", 
		"flow": flowId, 
		"node": nodeId
	};

	$.get(requestURL, request, successHandler, "json");
}

var createModelFromAjaxCall = function(data, model) {
	  var nodes = {};
  	  for (var i=0; i < data.nodes.length; ++i) {
  	  	var node = data.nodes[i];
  	  	nodes[node.id] = node;
  	  }
  	  for (var i=0; i < data.edges.length; ++i) {
  	  	var edge = data.edges[i];
  	  	var fromNode = nodes[edge.from];
  	  	var toNode = nodes[edge.target];
  	  	
  	  	if (!fromNode.outNodes) {
  	  		fromNode.outNodes = {};
  	  	}
  	  	fromNode.outNodes[toNode.id] = toNode;
  	  	
  	  	if (!toNode.inNodes) {
  	  		toNode.inNodes = {};
  	  	}
  	  	toNode.inNodes[fromNode.id] = fromNode;
  	  }
  
      console.log("data fetched");
      model.set({data: data});
      model.set({nodes: nodes});
      model.set({disabled: {}});
}

var nodeClickCallback = function(event, model, type) {
	console.log("Node clicked callback");
	var jobId = event.currentTarget.jobid;
	var flowId = model.get("flowId");
	var requestURL = contextURL + "/manager?project=" + projectName + 
			"&flow=" + flowId + "&job=" + jobId;

	if (event.currentTarget.jobtype == "flow") {
		var flowRequestURL = contextURL + "/manager?project=" + projectName + 
				"&flow=" + event.currentTarget.flowId;
		menu = [
				{title: "View Flow...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
				{break: 1},
				{title: "Open Flow...", callback: function() {window.location.href=flowRequestURL;}},
				{title: "Open Flow in New Window...", callback: function() {window.open(flowRequestURL);}},
				{break: 1},
				{title: "Open Properties...", callback: function() {window.location.href=requestURL;}},
				{title: "Open Properties in New Window...", callback: function() {window.open(requestURL);}},
				{break: 1},
				{title: "Center Flow", callback: function() {model.trigger("centerNode", jobId)}}
		];
	}
	else {
		menu = [
				{title: "View Job...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
				{break: 1},
				{title: "Open Job...", callback: function() {window.location.href=requestURL;}},
				{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
				{break: 1},
				{title: "Center Job", callback: function() {model.trigger("centerNode", jobId)}}
		];
	}
	contextMenuView.show(event, menu);
}

var jobClickCallback = function(event, model) {
	console.log("Node clicked callback");
	var jobId = event.currentTarget.jobid;
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobId;

	var menu;
	if (event.currentTarget.jobtype == "flow") {
		var flowRequestURL = contextURL + "/manager?project=" + projectName + "&flow=" + event.currentTarget.flowId;
		menu = [
				{title: "View Flow...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
				{break: 1},
				{title: "Open Flow...", callback: function() {window.location.href=flowRequestURL;}},
				{title: "Open Flow in New Window...", callback: function() {window.open(flowRequestURL);}},
				{break: 1},
				{title: "Open Properties...", callback: function() {window.location.href=requestURL;}},
				{title: "Open Properties in New Window...", callback: function() {window.open(requestURL);}},
				{break: 1},
				{title: "Center Flow", callback: function() {model.trigger("centerNode", jobId)}}
		];
	}
	else {
		menu = [
				{title: "View Job...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
				{break: 1},
				{title: "Open Job...", callback: function() {window.location.href=requestURL;}},
				{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
				{break: 1},
				{title: "Center Job", callback: function() {graphModel.trigger("centerNode", jobId)}}
		];
	}
	contextMenuView.show(event, menu);
}

var edgeClickCallback = function(event, model) {
	console.log("Edge clicked callback");
}

var graphClickCallback = function(event, model) {
	console.log("Graph clicked callback");
	var jobId = event.currentTarget.jobid;
	var flowId = model.get("flowId");
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId;

	var menu = [	
		{title: "Open Flow...", callback: function() {window.location.href=requestURL;}},
		{title: "Open Flow in New Window...", callback: function() {window.open(requestURL);}},
		{break: 1},
		{title: "Center Graph", callback: function() {model.trigger("resetPanZoom");}}
	];
	
	contextMenuView.show(event, menu);
}
