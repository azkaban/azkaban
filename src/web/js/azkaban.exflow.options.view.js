var executeFlowView;
var customSvgGraphView;
var customJobListView;
var cloneModel;

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

azkaban.ContextMenu = Backbone.View.extend({
	events : {
		"click #disableArrow" : "handleDisabledClick",
		"click #enableArrow" : "handleEnabledClick"
	},
	initialize: function(settings) {
		$('#disableSub').hide();
		$('#enableSub').hide();
	},
	handleEnabledClick: function(evt) {
		if(evt.stopPropagation) {
			evt.stopPropagation();
		}
		evt.cancelBubble=true;
		
		if (evt.currentTarget.expanded) {
			evt.currentTarget.expanded=false;
			$('#enableArrow').removeClass('collapse');
			$('#enableSub').hide();
		}
		else {
			evt.currentTarget.expanded=true;
			$('#enableArrow').addClass('collapse');
			$('#enableSub').show();
		}
	},
	handleDisabledClick: function(evt) {
		if(evt.stopPropagation) {
			evt.stopPropagation();
		}
		evt.cancelBubble=true;
		
		if (evt.currentTarget.expanded) {
			evt.currentTarget.expanded=false;
			$('#disableArrow').removeClass('collapse');
			$('#disableSub').hide();
		}
		else {
			evt.currentTarget.expanded=true;
			$('#disableArrow').addClass('collapse');
			$('#disableSub').show();
		}
	}
});

azkaban.ExecuteFlowView = Backbone.View.extend({
  	  events : {
  	  	"click" : "closeEditingTarget",
	    "click #execute-btn": "handleExecuteFlow",
	    "click #cancel-btn": "handleCancelExecution",
	    "click .modal-close": "handleCancelExecution",
	    "click #generalOptions": "handleGeneralOptionsSelect",
	    "click #flowOptions": "handleFlowOptionsSelect",
	    "click #addRow": "handleAddRow",
	    "click table .editable": "handleEditColumn",
	    "click table .removeIcon": "handleRemoveColumn"
	  },
	  initialize: function(setting) {
	  	 this.contextMenu = new azkaban.ContextMenu({el:$('#disableJobMenu')});
	  	 this.handleGeneralOptionsSelect();
	  },
	  show: function() {
	  	this.handleGeneralOptionsSelect();
	  	$('#modalBackground').show();
	  	$('#executing-options').show();
	  	this.cloneModel = this.model.clone();
	  	
	  	var fetchData = {"project": projectName, "ajax":"flowInfo", "flow":flowName};
	  	if (execId) {
	  		fetchData.execid = execId;
	  	}
	  	var executeURL = contextURL + "/executor";
	  	var handleAddRow = this.handleAddRow;
	  	
	  	var data = this.cloneModel.get("data");
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
	    this.cloneModel.set({nodes: nodes});
	  	
	  	$.get(
			executeURL,
			fetchData,
			function(data) {
				if (data.error) {
					alert(data.error);
				}
				else {
					$('#successEmails').val(data.successEmails.join());
					$('#failureEmails').val(data.failureEmails.join());
					
					if (data.failureAction) {
						$('#failureAction').val(data.failureAction);
					}
					if (data.notifyFailureFirst) {
						$('#notifyFailureFirst').attr('checked', true);
					}
					if (data.notifyFailureLast) {
						$('#notifyFailureLast').attr('checked', true);	
					}
					if (data.flowParam) {
						var flowParam = data.flowParam;
						for (var key in flowParam) {
							var row = handleAddRow();
							var td = $(row).find('td');
							$(td[0]).text(key);
							$(td[1]).text(flowParam[key]);
						}
					}

					if (!data.running || data.running.length == 0) {
						$(".radio").attr("disabled", "disabled");
						$(".radioLabel").addClass("disabled", "disabled");
					}
				}
			},
			"json"
		);
	  },
	  handleCancelExecution: function(evt) {
	  	var executeURL = contextURL + "/executor";
		$('#modalBackground').hide();
	  	$('#executing-options').hide();
	  },
	  handleGeneralOptionsSelect: function(evt) {
	  	$('#flowOptions').removeClass('selected');
	  	$('#generalOptions').addClass('selected');

	  	$('#generalPanel').show();	  	
	  	$('#graphPanel').hide();
	  },
	  handleFlowOptionsSelect: function(evt) {
	  	$('#generalOptions').removeClass('selected');
	  	$('#flowOptions').addClass('selected');

	  	$('#graphPanel').show();	  	
	  	$('#generalPanel').hide();
	  	
	  	if (this.flowSetup) {
	  		return;
	  	}
	  	
 	  	cloneModel = this.cloneModel;

		var disabled = {};
		var data = this.cloneModel.get("data");
		for (var i = 0; i < data.nodes.length; ++i) {
			var updateNode = data.nodes[i];
			if (updateNode.status == "DISABLED" || updateNode.status == "SUCCEEDED" || updateNode.status == "SKIPPED") {
				disabled[updateNode.id] = true;
			}
		}
 	  	cloneModel.set({disabled: disabled});
 	  	
	  	customSvgGraphView = new azkaban.SvgGraphView({el:$('#svgDivCustom'), model: this.cloneModel, rightClick: {id: 'disableJobMenu', callback: this.handleDisableMenuClick}});
		customJobsListView = new azkaban.JobListView({el:$('#jobListCustom'), model: this.cloneModel, rightClick: {id: 'disableJobMenu', callback: this.handleDisableMenuClick}});
		this.cloneModel.trigger("change:graph");
		
		this.flowSetup = true;
	  },
	  handleExecuteFlow: function(evt) {
	  	var executeURL = contextURL + "/executor";
	  	var disabled = this.cloneModel.get("disabled");
	  	var failureAction = $('#failureAction').val();
	  	var failureEmails = $('#failureEmails').val();
	  	var successEmails = $('#successEmails').val();
	  	var notifyFailureFirst = $('#notifyFailureFirst').is(':checked');
	  	var notifyFailureLast = $('#notifyFailureLast').is(':checked');
	  	var executingJobOption = $('input:radio[name=gender]:checked').val();
	  	
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
	  	
	  	var executingData = {
	  		project: projectName,
	  		ajax: "executeFlow",
	  		flow: flowName,
	  		disable: this.cloneModel.get('disabled'),
	  		failureAction: failureAction,
	  		failureEmails: failureEmails,
	  		successEmails: successEmails,
	  		notifyFailureFirst: notifyFailureFirst,
	  		notifyFailureLast: notifyFailureLast,
	  		executingJobOption: executingJobOption,
	  		flowOverride: flowOverride
	  	};
	  	
		$.get(
			executeURL,
			executingData,
			function(data) {
				if (data.error) {
					alert(data.error);
				}
				else {
					var redirectURL = contextURL + "/executor?execid=" + data.execid;
					window.location.href = redirectURL;
				}
			},
			"json"
		);
	  },
	  handleAddRow: function(evt) {
	  	var tr = document.createElement("tr");
	  	var tdName = document.createElement("td");
	    var tdValue = document.createElement("td");
	    
	    var icon = document.createElement("span");
	    $(icon).addClass("removeIcon");
	    var nameData = document.createElement("span");
	    $(nameData).addClass("spanValue");
	    var valueData = document.createElement("span");
	    $(valueData).addClass("spanValue");
	    	    
		$(tdName).append(icon);
		$(tdName).append(nameData);
		$(tdName).addClass("name");
		$(tdName).addClass("editable");
		
		$(tdValue).append(valueData);
	    $(tdValue).addClass("editable");
		
		$(tr).addClass("editRow");
	  	$(tr).append(tdName);
	  	$(tr).append(tdValue);
	   
	  	$(tr).insertBefore("#addRow");
	  	return tr;
	  },
	  handleEditColumn : function(evt) {
	  	var curTarget = evt.currentTarget;
	
	  	if (this.editingTarget != curTarget) {
			this.closeEditingTarget();
			
			var text = $(curTarget).children(".spanValue").text();
			$(curTarget).empty();
						
			var input = document.createElement("input");
			$(input).attr("type", "text");
			$(input).css("width", "100%");
			$(input).val(text);
			$(curTarget).addClass("editing");
			$(curTarget).append(input);
			$(input).focus();
			this.editingTarget = curTarget;
	  	}
	  },
	  handleRemoveColumn : function(evt) {
	  	var curTarget = evt.currentTarget;
	  	// Should be the table
	  	var row = curTarget.parentElement.parentElement;
		$(row).remove();
	  },
	  closeEditingTarget: function(evt) {
	  	if (this.editingTarget != null && this.editingTarget != evt.target && this.editingTarget != evt.target.parentElement ) {
	  		var input = $(this.editingTarget).children("input")[0];
	  		var text = $(input).val();
	  		$(input).remove();

		    var valueData = document.createElement("span");
		    $(valueData).addClass("spanValue");
		    $(valueData).text(text);

	  		if ($(this.editingTarget).hasClass("name")) {
		  		var icon = document.createElement("span");
		    	$(icon).addClass("removeIcon");
		    	$(this.editingTarget).append(icon);
		    }
		    
		    $(this.editingTarget).removeClass("editing");
		    $(this.editingTarget).append(valueData);
		    this.editingTarget = null;
	  	}
	  },
	  handleDisableMenuClick : function(action, el, pos) {
			var jobid = el[0].jobid;
			var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowName + "&job=" + jobid;
			if (action == "open") {
				window.location.href = requestURL;
			}
			else if(action == "openwindow") {
				window.open(requestURL);
			}
			else if(action == "disable") {
				var disabled = cloneModel.get("disabled");
		
				disabled[jobid] = true;
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if(action == "disableAll") {
				var disabled = cloneModel.get("disabled");
		
				var nodes = cloneModel.get("nodes");
				for (var key in nodes) {
					disabled[key] = true;
				}

				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "disableParents") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				var inNodes = nodes[jobid].inNodes;
		
				if (inNodes) {
					for (var key in inNodes) {
					  disabled[key] = true;
					}
				}
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "disableChildren") {
				var disabledMap = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				var outNodes = nodes[jobid].outNodes;
		
				if (outNodes) {
					for (var key in outNodes) {
					  disabledMap[key] = true;
					}
				}
				
				cloneModel.set({disabled: disabledMap});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "disableAncestors") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				
				recurseAllAncestors(nodes, disabled, jobid, true);
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "disableDescendents") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				
				recurseAllDescendents(nodes, disabled, jobid, true);
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if(action == "enable") {
				var disabled = cloneModel.get("disabled");
		
				disabled[jobid] = false;
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if(action == "enableAll") {
				disabled = {};
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "enableParents") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				var inNodes = nodes[jobid].inNodes;
		
				if (inNodes) {
					for (var key in inNodes) {
					  disabled[key] = false;
					}
				}
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "enableChildren") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				var outNodes = nodes[jobid].outNodes;
		
				if (outNodes) {
					for (var key in outNodes) {
					  disabled[key] = false;
					}
				}
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "enableAncestors") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				
				recurseAllAncestors(nodes, disabled, jobid, false);
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
			else if (action == "enableDescendents") {
				var disabled = cloneModel.get("disabled");
				var nodes = cloneModel.get("nodes");
				
				recurseAllDescendents(nodes, disabled, jobid, false);
				
				cloneModel.set({disabled: disabled});
				cloneModel.trigger("change:disabled");
			}
		}
});