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

function recurseAllAncestors(nodes, disabledMap, id, disable) {
	var node = nodes[id];
	
	if (node.in) {
		for (var key in node.in) {
			disabledMap[key] = disable;
			recurseAllAncestors(nodes, disabledMap, key, disable);
		}
	}
}

function recurseAllDescendents(nodes, disabledMap, id, disable) {
	var node = nodes[id];
	
	if (node.out) {
		for (var key in node.out) {
			disabledMap[key] = disable;
			recurseAllDescendents(nodes, disabledMap, key, disable);
		}
	}
}

var flowExecuteDialogView;
azkaban.FlowExecuteDialogView= Backbone.View.extend({
  events : {
  	"click .closeExecPanel": "hideExecutionOptionPanel",
  	"click #schedule-btn" : "scheduleClick",
  	"click #execute-btn" : "handleExecuteFlow"
  },
  initialize : function(settings) {
  	  this.model.bind('change:flowinfo', this.changeFlowInfo, this);
  	  
  	  $("#overrideSuccessEmails").click(function(evt) {
		 if($(this).is(':checked')){
		 	$('#successEmails').attr('disabled',null);
		 }
		 else {
		 	$('#successEmails').attr('disabled',"disabled");
		 }
  	  });
  	  
  	   $("#overrideFailureEmails").click(function(evt) {
		 if($(this).is(':checked')){
		 	$('#failureEmails').attr('disabled',null);
		 }
		 else {
		 	$('#failureEmails').attr('disabled',"disabled");
		 }
  	  });
  },
  render: function() {
  },
  getExecutionOptionData: function() {
	var failureAction = $('#failureAction').val();
	var failureEmails = $('#failureEmails').val();
	var successEmails = $('#successEmails').val();
	var notifyFailureFirst = $('#notifyFailureFirst').is(':checked');
	var notifyFailureLast = $('#notifyFailureLast').is(':checked');
	var failureEmailsOverride = $("#overrideFailureEmails").is(':checked');
	var successEmailsOverride = $("#overrideSuccessEmails").is(':checked');
	
	var flowOverride = {};
	var editRows = $(".editRow");
	for (var i = 0; i < editRows.length; ++i) {
		var row = editRows[i];
		var td = $(row).find('td');
		var key = $(td[0]).text();
		var val = $(td[1]).text();
		
		if (key && key.length > 0) {
			flowOverride[key] = val;
		}
	}
	
	var disabled = "";
	var disabledMap = this.model.get('disabled');
	for (var dis in disabledMap) {
		if (disabledMap[dis]) {
			disabled += dis + ",";
		}
	}
	
	var executingData = {
		projectId: projectId,
		project: this.projectName,
		ajax: "executeFlow",
		flow: this.flowId,
		disabled: disabled,
		failureEmailsOverride:failureEmailsOverride,
		successEmailsOverride:successEmailsOverride,
		failureAction: failureAction,
		failureEmails: failureEmails,
		successEmails: successEmails,
		notifyFailureFirst: notifyFailureFirst,
		notifyFailureLast: notifyFailureLast,
		flowOverride: flowOverride
	};
	
	// Set concurrency option, default is skip

	var concurrentOption = $('input[name=concurrent]:checked').val();
	executingData.concurrentOption = concurrentOption;
	if (concurrentOption == "pipeline") {
		var pipelineLevel = $("#pipelineLevel").val();
		executingData.pipelineLevel = pipelineLevel;
	}
	else if (concurrentOption == "queue") {
		executingData.queueLevel = $("#queueLevel").val();
	}
	
	return executingData;
  },
  changeFlowInfo: function() {
  	var successEmails = this.model.get("successEmails");
  	var failureEmails = this.model.get("failureEmails");
  	var failureActions = this.model.get("failureAction");
  	var notifyFailure = this.model.get("notifyFailure");
  	var flowParams = this.model.get("flowParams");
  	var isRunning = this.model.get("isRunning");
  	var concurrentOption = this.model.get("concurrentOption");
  	var pipelineLevel = this.model.get("pipelineLevel");
  	var pipelineExecutionId = this.model.get("pipelineExecution");
  	var queueLevel = this.model.get("queueLevel");
  	var nodeStatus = this.model.get("nodeStatus");
  	var overrideSuccessEmails = this.model.get("overrideSuccessEmails");
  	var overrideFailureEmails = this.model.get("overrideFailureEmails");
  	
	if (overrideSuccessEmails) {
		$('#overrideSuccessEmails').attr('checked', true);
	}
	else {
		$('#successEmails').attr('disabled','disabled');
	}
	if (overrideFailureEmails) {
		$('#overrideFailureEmails').attr('checked', true);
	}
	else {
		$('#failureEmails').attr('disabled','disabled');
	}
  	
  	if (successEmails) {
  		$('#successEmails').val(successEmails.join());
  	}
  	if (failureEmails) {
  		$('#failureEmails').val(failureEmails.join());
  	}
  	if (failureActions) {
		$('#failureAction').val(failureActions);
  	}
  	
  	if (notifyFailure.first) {
		$('#notifyFailureFirst').attr('checked', true);
  	}
  	if (notifyFailure.last) {
  		$('#notifyFailureLast').attr('checked', true);
  	}
  	
  	if (concurrentOption) {
  		$('input[value='+concurrentOption+'][name="concurrent"]').attr('checked', true);
  	}
  	if (pipelineLevel) {
  		$('#pipelineLevel').val(pipelineLevel);
  	}
  	if (queueLevel) {
  		$('#queueLevel').val(queueLevel);
  	}
  	
  	if (nodeStatus) {
  		var nodeMap = this.model.get("nodeMap");
  
  		var disabled = {};
  		for (var key in nodeStatus) {
  			var status = nodeStatus[key];
  			
  			var node = nodeMap[key];
  			if (node) {
  				node.status = status;
  				
				if (node.status == "DISABLED" || node.status == "SKIPPED") {
					node.status = "READY";
					disabled[node.id] = true;
				}
				if (node.status == "SUCCEEDED" || node.status=="RUNNING") {
					disabled[node.id] = true;
				}
  			}
  		}
  		this.model.set({"disabled":disabled});
  	}
  	
	if (flowParams) {
		for (var key in flowParams) {
			editTableView.handleAddRow({paramkey: key, paramvalue: flowParams[key]});
		}
	}
  },
  show: function(data) {
  	var projectName = data.project;
  	var flowId = data.flow;
  	var jobId = data.job;
  	
  	// ExecId is optional
  	var execId = data.execid;
  
  	var loadedId = executableGraphModel.get("flowId");
  
  	this.loadGraph(projectName, flowId);
  	this.loadFlowInfo(projectName, flowId, execId);

  	this.projectName = projectName;
  	this.flowId = flowId;
	if (jobId) {
		this.showExecuteJob(projectName, flowId, jobId, data.withDep);
	}
	else {
		this.showExecuteFlow(projectName, flowId);
	}
  },
  showExecuteFlow: function(projectName, flowId) {
	$("#execute-flow-panel-title").text("Execute Flow " + flowId);
	this.showExecutionOptionPanel();

	// Triggers a render
	this.model.trigger("change:graph");
  },
  showExecuteJob: function(projectName, flowId, jobId, withDep) {
	sideMenuDialogView.menuSelect($("#flowOption"));
	$("#execute-flow-panel-title").text("Execute Flow " + flowId);
	
	var nodes = this.model.get("nodeMap");
	var disabled = this.model.get("disabled");
	
	// Disable all, then re-enable those you want.
	for(var key in nodes) {
		disabled[key] = true;
	}
	
	var jobNode = nodes[jobId];
	disabled[jobId] = false;
	
	if (withDep) {
		recurseAllAncestors(nodes, disabled, jobId, false);
	}

	this.showExecutionOptionPanel();
	this.model.trigger("change:graph");
  },
  showExecutionOptionPanel: function() {
  	sideMenuDialogView.menuSelect($("#flowOption"));
  	$('#modalBackground').show();
  	$('#execute-flow-panel').show();
  },
  hideExecutionOptionPanel: function() {
  	$('#modalBackground').hide();
  	$('#execute-flow-panel').hide();
  },
  scheduleClick: function() {
  	schedulePanelView.showSchedulePanel();
  },
  loadFlowInfo: function(projectName, flowId, execId) {
    console.log("Loading flow " + flowId);
	fetchFlowInfo(this.model, projectName, flowId, execId);
  },
  loadGraph: function(projectName, flowId) {
  	console.log("Loading flow " + flowId);
	fetchFlow(this.model, projectName, flowId, true);
  },
  handleExecuteFlow: function(evt) {
  	  	var executeURL = contextURL + "/executor";
		var executingData = this.getExecutionOptionData();
		executeFlow(executingData);
  }
});

var editTableView;
azkaban.EditTableView = Backbone.View.extend({
	events : {
		"click table .addRow": "handleAddRow",
		"click table .editable": "handleEditColumn",
		"click table .removeIcon": "handleRemoveColumn"
	},
	initialize: function(setting) {

	},
	handleAddRow: function(data) {
		var name = "";
		if (data.paramkey) {
			name = data.paramkey;
		}
		
		var value = "";
		if (data.paramvalue) {
			value = data.paramvalue;
		}
	
	  	var tr = document.createElement("tr");
	  	var tdName = document.createElement("td");
	    var tdValue = document.createElement("td");
	    
	    var icon = document.createElement("span");
	    $(icon).addClass("removeIcon");
	    var nameData = document.createElement("span");
	    $(nameData).addClass("spanValue");
	    $(nameData).text(name);
	    var valueData = document.createElement("span");
	    $(valueData).addClass("spanValue");
	    $(valueData).text(value);
	    	    
		$(tdName).append(icon);
		$(tdName).append(nameData);
		$(tdName).addClass("name");
		$(tdName).addClass("editable");
		
		$(tdValue).append(valueData);
	    $(tdValue).addClass("editable");
		
		$(tr).addClass("editRow");
	  	$(tr).append(tdName);
	  	$(tr).append(tdValue);
	   
	  	$(tr).insertBefore(".addRow");
	  	return tr;
	  },
	  handleEditColumn : function(evt) {
	  	var curTarget = evt.currentTarget;
	
		var text = $(curTarget).children(".spanValue").text();
		$(curTarget).empty();
					
		var input = document.createElement("input");
		$(input).attr("type", "text");
		$(input).css("width", "100%");
		$(input).val(text);
		$(curTarget).addClass("editing");
		$(curTarget).append(input);
		$(input).focus();
		
		var obj = this;
		$(input).focusout(function(evt) {
			obj.closeEditingTarget(evt);
		});
		
		$(input).keypress(function(evt) {
		    if(evt.which == 13) {
		        obj.closeEditingTarget(evt);
		    }
		});

	  },
	  handleRemoveColumn : function(evt) {
	  	var curTarget = evt.currentTarget;
	  	// Should be the table
	  	var row = curTarget.parentElement.parentElement;
		$(row).remove();
	  },
	  closeEditingTarget: function(evt) {
  		var input = evt.currentTarget;
  		var text = $(input).val();
  		var parent = $(input).parent();
  		$(parent).empty();

	    var valueData = document.createElement("span");
	    $(valueData).addClass("spanValue");
	    $(valueData).text(text);

		if($(parent).hasClass("name")) {
			var icon = document.createElement("span");
			$(icon).addClass("removeIcon");
			$(parent).append(icon);
		}
	    
	    $(parent).removeClass("editing");
	    $(parent).append(valueData);
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
	
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowName + "&job=" + jobid;
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
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobId;

	var menu = [	{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
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
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId;
	
	var menu = [	{title: "Open Flow in New Window...", callback: function() {window.open(requestURL);}},
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
	executableGraphModel = new azkaban.GraphModel();
	flowExecuteDialogView = new azkaban.FlowExecuteDialogView({el:$('#execute-flow-panel'), model: executableGraphModel});
	svgGraphView = new azkaban.SvgGraphView({el:$('#svgDivCustom'), model: executableGraphModel, topGId:"topG", graphMargin: 10, rightClick: { "node": nodeClickCallback, "edge": edgeClickCallback, "graph": graphClickCallback }});
	
	sideMenuDialogView = new azkaban.SideMenuDialogView({el:$('#graphOptions')});
	editTableView = new azkaban.EditTableView({el:$('#editTable')});

	contextMenuView = new azkaban.ContextMenuView({el:$('#contextMenu')});
});
