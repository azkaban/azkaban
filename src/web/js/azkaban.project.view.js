$.namespace('azkaban');

var projectView;
azkaban.ProjectView= Backbone.View.extend({
  events : {
      "click #project-upload-btn":"handleUploadProjectJob"
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

var flowTableView;
azkaban.FlowTableView= Backbone.View.extend({
  events : {
    "click .jobfolder": "expandFlowProject",
    "hover .expandedFlow a": "highlight"
  },
  initialize : function(settings) {
  	_.bindAll(this, 'createJobListTable');
  },
  expandFlowProject : function(evt) {
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
	      {"project": projectId, "json":"fetchflowlist", "flow":targetId},
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
  	
  	for (var i = 0; i < nodes.length; i++) {
		var job = nodes[i];
		var name = job.id;
		var level = job.level;
		var nodeId = flowId + "-" + name;
		
		var tr = document.createElement("tr");
		var idtd = document.createElement("td");
		$(idtd).addClass("tb-name");
		
		var ida = document.createElement("a");
		ida.dependents = job.dependents;
		ida.dependencies = job.dependencies;
		ida.flowid = flowId;
		$(ida).text(name);
		$(ida).attr("id", nodeId);
		$(ida).css("margin-left", level * 20);
		
		$(idtd).append(ida);
		$(tr).append(idtd);
		$(innerTable).append(tr);
  	}
  },
  highlight: function(evt) {
 	var currentTarget = evt.currentTarget;
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
  render: function() {
  }
});

$(function() {
	projectView = new azkaban.ProjectView({el:$( '#all-jobs-content'), successMsg: successMessage, errorMsg: errorMessage });
	uploadView = new azkaban.UploadProjectView({el:$('#upload-project')});
	flowTableView = new azkaban.FlowTableView({el:$('#flow-tabs')});
	// Setting up the project tabs

});
