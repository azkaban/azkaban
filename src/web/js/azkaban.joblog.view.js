$.namespace('azkaban');

var logModel;
azkaban.LogModel = Backbone.Model.extend({});

var jobLogView;
azkaban.JobLogView = Backbone.View.extend({
	events: {
		"click #updateLogBtn" : "handleUpdate"
	},
	initialize: function(settings) {
		this.model.set({"current": 0});
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var current = this.model.get("current");
		var requestURL = contextURL + "/executor"; 
		var model = this.model;

		ajaxCall(
			requestURL,
			{"execid": execId, "job": jobId, "ajax":"fetchExecJobLogs", "current": current, "max": 100000},
			function(data) {
	          console.log("fetchLogs");
	          if (data.error) {
	          	showDialog("Error", data.error);
	          }
	          else {
	          	var log = $("#logSection").text();
	          	if (!log) {
	          		log = data.log;
	          	}
	          	else {
	          		log += data.log;
	          	}
	          	
	          	current = data.current;
	          	$("#logSection").text(log);
	          	model.set({"current": current, "log": log});
	          }
	      }
	    );
	}
});

$(function() {
	var selected;

	logModel = new azkaban.LogModel();
	jobLogView = new azkaban.JobLogView({el:$('#jobLogView'), model: logModel});
});
