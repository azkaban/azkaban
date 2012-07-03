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
    "click .jobfolder": "expandFlowProject"
  },
  initialize : function(settings) {
  },
  expandFlowProject : function(evt) {
    var target = evt.currentTarget;
    var targetId = target.id;
    var requestURL = contextURL + "/manager";

    var targetExpanded = $('#' + targetId + '-child');
    
    if (target.loading) {
    	console.log("Still loading.");
    }
    else if (target.loaded) {
    	if($(targetExpanded).is(':visible')) {
    		$(target).addClass('expand').removeClass('collapse');
    		$(targetExpanded).slideUp("fast");
    	}
    	else {
    	    $(target).addClass('collapse').removeClass('expand');
    		$(targetExpanded).slideDown("fast");
    	}
    }
    else {
	    // projectId is available
	    $(target).addClass('wait').removeClass('collapse').removeClass('expand');
	    target.loading = true;
	    $.get(
	      requestURL,
	      {"project": projectId, "json":"expandflow", "flow":targetId},
	      function(data) {
	        console.log("Success");
	        target.loaded = true;
	        target.loading = false;
			$(target).addClass('collapse').removeClass('wait');
	    	$(targetExpanded).slideDown("fast");
	      },
	      "json"
	    );
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
