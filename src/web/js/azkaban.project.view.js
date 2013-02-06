$.namespace('azkaban');

var projectView;
azkaban.ProjectView= Backbone.View.extend({
  events : {
      "click #project-upload-btn":"handleUploadProjectJob",
      "click #project-delete-btn": "handleDeleteProject"
  },
  initialize : function(settings) {
  },
  handleUploadProjectJob : function(evt) {
      console.log("click upload project");
      $('#upload-project').modal({
          closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
          position: ["20%",],
          containerId: 'confirm-container',
          containerCss: {
            'height': '220px',
            'width': '565px'
          },
          onShow: function (dialog) {
            var modal = this;
            $("#errorMsg").hide();
          }
        });
  },
  handleDeleteProject : function(evt) {
	$('#delete-project').modal({
        closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
        position: ["20%",],
        containerId: 'confirm-container',
        containerCss: {
          'height': '240px',
          'width': '640px'
        },
        onShow: function (dialog) {
          var modal = this;
          $("#errorMsg").hide();
        }
    });
  },
  render: function() {
  }
});

var uploadProjectView;
azkaban.UploadProjectView= Backbone.View.extend({
  events : {
    "click #upload-btn": "handleCreateProject"
  },
  initialize : function(settings) {
    $("#errorMsg").hide();
  },
  handleCreateProject : function(evt) {
    $("#upload-form").submit();
  },
  render: function() {
  }
});

var deleteProjectView;
azkaban.DeleteProjectView= Backbone.View.extend({
  events : {
    "click #delete-btn": "handleDeleteProject"
  },
  initialize : function(settings) {
  },
  handleDeleteProject : function(evt) {
  	$("#delete-form").submit();
  },
  render: function() {
  }
});

var flowTableView;
azkaban.FlowTableView= Backbone.View.extend({
  events : {
    "click .jobfolder": "expandFlowProject",
    "hover .expandedFlow a": "highlight",
    "hover .row .tb-name": "flowRunOptions",
    "hover .innerTable .tb-job-name": "jobRunOptions",
    "click .runJob": "runJob",
    "click .runWithDep": "runWithDep",
    "click .executeFlow": "executeFlow"
  },
  initialize : function(settings) {
  	_.bindAll(this, 'createJobListTable');
  },
  expandFlowProject : function(evt) {
    if (evt.target.tagName!="SPAN") {
    	return;
    }
    
    var target = evt.currentTarget;
    var targetId = target.id;
    var requestURL = contextURL + "/manager";

    var targetExpanded = $('#' + targetId + '-child');
    var targetTBody = $('#' + targetId + '-tbody');
    
    var createJobListFunction = this.createJobListTable;
    
    if (target.loading) {
    	console.log("Still loading.");
    }
    else if (target.loaded) {
    	if($(targetExpanded).is(':visible')) {
    		$(target).addClass('expand').removeClass('collapse');
    		$(targetExpanded).fadeOut("fast");
    	}
    	else {
    	    $(target).addClass('collapse').removeClass('expand');
    		$(targetExpanded).fadeIn();
    	}
    }
    else {
	    // projectId is available
	    $(target).addClass('wait').removeClass('collapse').removeClass('expand');
	    target.loading = true;
	    
	    $.get(
	      requestURL,
	      {"project": projectId, "ajax":"fetchflowjobs", "flow":targetId},
	      function(data) {
	        console.log("Success");
	        target.loaded = true;
	        target.loading = false;
	        
	        createJobListFunction(data, targetTBody);
	        
			$(target).addClass('collapse').removeClass('wait');
	    	$(targetExpanded).fadeIn("fast");
	      },
	      "json"
	    );
    }
  },
  createJobListTable : function(data, innerTable) {
  	var nodes = data.nodes;
  	var flowId = data.flowId;
  	var project = data.project;
  	var requestURL = contextURL + "/manager?project=" + project + "&flow=" + flowId + "&job=";
  	for (var i = 0; i < nodes.length; i++) {
		var job = nodes[i];
		var name = job.id;
		var level = job.level;
		var nodeId = flowId + "-" + name;
		
		var tr = document.createElement("tr");
		var idtd = document.createElement("td");
		$(idtd).addClass("tb-name");
		$(idtd).addClass("tb-job-name");
		idtd.flowId=flowId;
		idtd.projectName=project;
		idtd.jobName=name;
		
		var ida = document.createElement("a");
		ida.dependents = job.dependents;
		ida.dependencies = job.dependencies;
		ida.flowid = flowId;
		$(ida).text(name);
		$(ida).attr("id", nodeId);
		$(ida).css("margin-left", level * 20);
		$(ida).attr("href", requestURL + name);
		
		$(idtd).append(ida);
		$(tr).append(idtd);
		$(innerTable).append(tr);
  	}
  },
  flowRunOptions: function(evt) {
  	var hover = evt.type == "mouseover";
  	var projectName = $(evt.currentTarget).attr("project");
  	var flowId = $(evt.currentTarget).attr("flow");
  	
  	var menuName = flowId + "-hover-menu";
  	if (hover) {
  		var divMenu = document.createElement("div");
  		$(divMenu).attr("id", menuName);
  		$(divMenu).addClass("job-hover-menu");
  		
  		var divRunJob = document.createElement("div");
  		$(divRunJob).addClass("btn1");
  		$(divRunJob).addClass("executeFlow");
  		$(divRunJob).text("Execute Flow");
  		divRunJob.flowId = flowId;
  		$(divMenu).append(divRunJob);

  		$(evt.currentTarget).append(divMenu);
  	}
  	else {
  		var menu = $("#" + menuName);
  		$(menu).remove();
  	}
  },
  jobRunOptions: function(evt) {
    var projectName = evt.currentTarget.projectName;
  	var flowId = evt.currentTarget.flowId;
  	var jobName = evt.currentTarget.jobName;
  	console.log("job run options " + projectName + ":" + flowId + ":" + jobName);
  	
  	var hover = evt.type == "mouseover";
  	
  	var menuName = flowId + "-" + jobName + "-hover-menu";
  	if (hover) {
  		var divMenu = document.createElement("div");
  		$(divMenu).attr("id", menuName);
  		$(divMenu).addClass("job-hover-menu");

  		var divRunJob = document.createElement("div");
  		$(divRunJob).addClass("btn1");
  		$(divRunJob).addClass("runJob");
  		$(divRunJob).text("Run Job");
  		divRunJob.jobName = jobName;
  		divRunJob.flowId = flowId;
  		$(divMenu).append(divRunJob);
  		
  		var divRunWithDep = document.createElement("div");
  		$(divRunWithDep).addClass("btn1");
  		$(divRunWithDep).addClass("runWithDep");
  		$(divRunWithDep).text("Run With Dependencies");
  		divRunWithDep.jobName = jobName;
  		divRunWithDep.flowId = flowId;
  		$(divMenu).append(divRunWithDep);
  		
  		$(evt.currentTarget).append(divMenu);
  	}
  	else {
  		var menu = $("#" + menuName);
  		$(menu).remove();
  	}
  	
  },
  highlight: function(evt) {
 	var currentTarget = evt.currentTarget;
	this.highlightJob(currentTarget);
  },
  highlightJob: function(currentTarget) {
   	var dependents = currentTarget.dependents;
 	var dependencies = currentTarget.dependencies;
 	var flowid = currentTarget.flowid;
 	
 	if (dependents) {
	 	for (var i = 0; i < dependents.length; ++i) {
	 		var depId = flowid + "-" + dependents[i];
	 		$("#"+depId).toggleClass("dependent");
	 	}
 	}
 	
 	if (dependencies) {
	 	for (var i = 0; i < dependencies.length; ++i) {
	 		var depId = flowid + "-" + dependencies[i];
	 		$("#"+depId).toggleClass("dependency");
	 	}
  	}
  },
  runJob: function(evt) {
  	console.log("Run Job");
  	var jobId = evt.currentTarget.jobName;
  	var flowId = evt.currentTarget.flowId;
  	
  	$("#execute-message").text("Execute only job '" + jobId + "' in flow '" + flowId + "'.");
  	
  	var executingData = {
  		project: projectId,
  		ajax: "executeFlow",
  		flow: flowId,
  		job: jobId
	};
  	
  	this.executeFlowDialog(executingData);
  },
  runWithDep: function(evt) {
    var jobId = evt.currentTarget.jobName;
  	var flowId = evt.currentTarget.flowId;
    console.log("Run With Dep");
    $("#execute-message").text("Execute job '" + jobId + "' and all of its ancestors in '" + flowId + "'.");
    
    var executingData = {
  		project: projectId,
  		ajax: "executeFlow",
  		flow: flowId,
  		job: jobId,
  		withDep: true
	};
    
    this.executeFlowDialog(executingData);
  },
  executeFlow: function(evt) {
    console.log("Execute Flow");
    var flowId = evt.currentTarget.flowId;
    $("#execute-message").text("Execute the complete flow '" + flowId + "'.");
    
    var executingData = {
  		project: projectId,
  		ajax: "executeFlow",
  		flow: flowId
	};
    
    this.executeFlowDialog(executingData);
  },
  executeFlowDialog: function(executingData) {
  	var flowId = executingData.flow;
  	var executionIds = flowExecutingStatus(projectId, flowId);
  	
    if (executionIds && executionIds.length > 0) {
    	$("#executeErrorMsg").text("Flow '" + flowId + "' is already running. Click on Execute to proceed anyways.");
    }
    else {
    	$("#executeErrorMsg").hide();
    }

 	$('#flow-execute').modal({
      closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
      position: ["20%",],
      containerId: 'confirm-container',
      containerCss: {
        'height': '220px',
        'width': '565px'
      },
      onShow: function (dialog) {
        var modal = this;
        $('#execute-btn').click(function() {
        	executeFlow(executingData);
        });
      }
    });
  },
  render: function() {
  }
});

var projectSummary;
azkaban.ProjectSummaryView= Backbone.View.extend({
  events : {
      "click #edit": "handleDescriptionEdit"
  },
  initialize : function(settings) {
  },
  handleDescriptionEdit : function(evt) {
      console.log("Edit description");
      var editText = $("#edit").text();
      var descriptionTD = $('#pdescription');
      
      if (editText != "Edit Description") {
          var requestURL = contextURL + "/manager";
          var newText = $("#descEdit").val();

          $.get(
		      requestURL,
		      {"project": projectId, "ajax":"changeDescription", "description":newText},
		      function(data) {
				if (data.error) {
					alert(data.error);
				}
		      },
		      "json"
	    );
          
          $(descriptionTD).remove("#descEdit");
          $(descriptionTD).text(newText);
          
          $("#edit").text("Edit Description");
      }
      else {
	      var text = $(descriptionTD).text();
	      var edit = document.createElement("textarea");
	      
	      $(edit).addClass("editTextArea");
	      $(edit).attr("id", "descEdit");
	      $(edit).val(text);
	      $(descriptionTD).text("");
	      $(descriptionTD).append(edit);
	      
	      $("#edit").text("Commit");
      }
  },
  render: function() {
  }
});

$(function() {
	projectView = new azkaban.ProjectView({el:$('#all-jobs-content')});
	uploadView = new azkaban.UploadProjectView({el:$('#upload-project')});
	flowTableView = new azkaban.FlowTableView({el:$('#flow-tabs')});
	projectSummary = new azkaban.ProjectSummaryView({el:$('#project-summary')});
	deleteProjectView = new azkaban.DeleteProjectView({el: $('#delete-project')});
	// Setting up the project tabs
});
