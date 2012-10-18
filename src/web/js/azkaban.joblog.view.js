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

		ajaxLogsCall(
			requestURL,
			{"execid": execId, "job": jobId, "ajax":"fetchExecJobLogs", "current": current, "max": 100000},
			function(data) {
	          console.log("fetchLogs");
	          if (data.error) {
	          	showDialog("Error", data.error);
	          }
	          else {
			var re = /(https?:\/\/(([-\w\.]+)+(:\d+)?(\/([\w/_\.]*(\?\S+)?)?)?))/g;
	          	var log = $("#logSection").text();
	          	if (!log) {
	          		log = data.log;
	          	}
	          	else {
	          		log += data.log;
	          	}
    	
	          	current = data.current;

	          	$("#logSection").text(log);
			log = $("#logSection").html();
			log = log.replace(re, "<a href=\"$1\" title=\"\">$1</a>");
			$("#logSection").html(log);

	          	model.set({"current": current, "log": log});
	          	$(".logViewer").scrollTop(9999);
	          }
	      }
	    );
	}
});

var showDialog = function(title, message) {
  $('#messageTitle').text(title);

  $('#messageBox').text(message);

  $('#messageDialog').modal({
      closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
      position: ["20%",],
      containerId: 'confirm-container',
      containerCss: {
        'height': '220px',
        'width': '565px'
      },
      onShow: function (dialog) {
      }
    });
}


$(function() {
	var selected;

	logModel = new azkaban.LogModel();
	jobLogView = new azkaban.JobLogView({el:$('#jobLogView'), model: logModel});
});
