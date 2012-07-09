$.namespace('azkaban');

var flowView;
azkaban.FlowView= Backbone.View.extend({
  events : {
  	"click #graphViewLink" : "handleGraphLinkClick",
  	"click #jobslistViewLink" : "handleJobslistLinkClick"
  },
  initialize : function(settings) {
  	var selectedView = settings.selectedView;
  	if (selectedView == "jobslist") {
  		this.handleJobslistLinkClick();
  	}
  	else {
  		this.handleGraphLinkClick();
  	}

  },
  render: function() {
  
  },
  handleGraphLinkClick: function(){
  	$("#jobslistViewLink").removeClass("selected");
  	$("#graphViewLink").addClass("selected");
  	
  	$("#jobListView").hide();
  	$("#graphView").show();
  },
  handleJobslistLinkClick: function() {
  	$("#graphViewLink").removeClass("selected");
  	$("#jobslistViewLink").addClass("selected");
  	
  	 $("#graphView").hide();
  	 $("#jobListView").show();
  }
});

$(function() {
	var selected;
	if (window.location.hash) {
		var hash = window.location.hash;
		if (hash == "#jobslist") {
			selected = "jobslist";
		}
		else if (hash == "#graph") {
			// Redundant, but we may want to change the default. 
			selected = "graph";
		}
		else {
			selected = "graph";
		}
	}
	flowView = new azkaban.FlowView({el:$( '#all-jobs-content'), selectedView: selected });
});
