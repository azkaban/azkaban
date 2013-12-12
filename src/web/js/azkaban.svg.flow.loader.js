var extendedViewPanels = {};
var extendedDataModels = {};
var openJobDisplayCallback = function(nodeId, flowId, evt) {
	console.log("Open up data");
	var target = evt.currentTarget;
	var node = target.nodeobj;
	
	// If target panel exists, than we display and skip.
	var targetPanel = node.panel;
	if (targetPanel) {
		$("#flowInfoBase").before(targetPanel);
		targetPanel.showExtendedView(evt);
	}
	else {
		var targetModel = node.dataModel;
		var flowId = flowId;
		
		if (!targetModel) {
			var requestURL = contextURL + "/manager";
			var newParentPath = node.parentPath ? node.parentPath + ":" + flowId : flowId;
			node.parentPath = newParentPath;
			
			$.get(
		      requestURL,
		      {"project": projectName, "ajax":"fetchflownodedata", "flow":flowId, "node": node.id},
		      function(data) {
		  		var graphModel = new azkaban.GraphModel();
		  		graphModel.set({id: data.id, flow: data.flowData, type: data.type, props: data.props});
				
		  		var flowData = data.flowData;
		  		if (flowData) {
		  			parseFlowData(flowData, graphModel, newParentPath);
		  		}
		  		
		  		node.dataModel = graphModel;
		  		createNewPanel(node, graphModel, evt);
		      },
		      "json"
		    );
		}
		else {
			createNewPanel(node, targetModel, evt);
		}
	}

	/*
	$("#flowInfoBase").before(cloneStuff);
	var requestURL = contextURL + "/manager";
	
	$.get(
      requestURL,
      {"project": projectName, "ajax":"fetchflownodedata", "flow":flowId, "node": nodeId},
      function(data) {
  		var graphModel = new azkaban.GraphModel();
  		graphModel.set({id: data.id, flow: data.flowData, type: data.type, props: data.props});

  		var flowData = data.flowData;
  		if (flowData) {
  			createModelFromAjaxCall(flowData, graphModel);
  		}
  		
  		var backboneView = new azkaban.FlowExtendedViewPanel({el:cloneStuff, model: graphModel});
  		extendedViewPanels[nodeInfoPanelID] = backboneView;
  		extendedDataModels[nodeInfoPanelID] = graphModel;
  		backboneView.showExtendedView(evt);
      },
      "json"
    );
    */
}

var createNewPanel = function(node, model, evt) {
	var parentPath = node.parentPath;
	
	var nodeInfoPanelID = parentPath ? parentPath + ":" + node.id + "-info" : node.id + "-info";
	var cloneStuff = $("#flowInfoBase").clone();
	cloneStuff.nodeobj = node;
	$(cloneStuff).attr("id", nodeInfoPanelID);
	$("#flowInfoBase").before(cloneStuff);
	
	var backboneView = new azkaban.FlowExtendedViewPanel({el:cloneStuff, model: model});
	node.panel = backboneView;
	backboneView.showExtendedView(evt);
}

var parseFlowData = function(data, model, parentPath) {
	var nodes = {};
	var edges = new Array();
	for (var i=0; i < data.nodes.length; ++i) {
		var node = data.nodes[i];
		nodes[node.id] = node;
	}

    var nodeQueue = new Array();
	for (var i=0; i < data.nodes.length; ++i) {
		var node = data.nodes[i];
		if (node.in) {
			for (var j=0; j < node.in.length; ++j) {
				var fromNode = nodes[node.in[j]];
				if (!fromNode.outNodes) {
					fromNode.outNodes = {};
				}
				if (!node.inNodes) {
					node.inNodes = {};
				}
				
				fromNode.outNodes[node.id] = node;
				node.inNodes[fromNode.id] = fromNode;
				edges.push({to: node.id, from: fromNode.id});
			}
		}
		else {
			// Queue used for breath first.
			nodeQueue.push(node);
		}
	}

	// Iterate over the nodes again
	var embeddedFlows = {};
	var newParentPath = parentPath ? parentPath + ":" + data.flow : data.flow;
	
	for (var key in nodes) {
		var node = nodes[key];
		node.parentPath = newParentPath;
		if (node.type == "flow" && node.flowData) {
			var graphModel = new azkaban.GraphModel();

			node.flowData.id = node.id;
			node.flowData.flowId = node.flowId;
			parseFlowData(node.flowData, graphModel, newParentPath);
			graphModel.set({id: node.id, flow: node.flowData, type: node.type, props: node.props});
			graphModel.set({isEmbedded: true});
			node.dataModel = graphModel;
		}
	}
	
	console.log("data fetched");
	model.set({flow: data.flow});
	model.set({data: data});
	model.set({nodes: nodes});
	model.set({edges: edges});
	model.set({disabled: {}});
}

var closeAllSubDisplays = function() {
	$(".flowExtendedView").hide();
}

var nodeClickCallback = function(event, model, type) {
	console.log("Node clicked callback");
	var target = event.currentTarget;
	var jobId = target.jobid;
	var flowId = model.get("flow");
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobId;

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
