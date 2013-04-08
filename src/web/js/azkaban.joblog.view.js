$.namespace('azkaban');

var logModel;
azkaban.LogModel = Backbone.Model.extend({});

var jobLogView;
azkaban.JobLogView = Backbone.View.extend({
	events: {
		"click #updateLogBtn" : "handleUpdate"
	},
	initialize: function(settings) {
		this.model.set({"offset": 0});
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var requestURL = contextURL + "/executor"; 
		var model = this.model;
		var finished = false;

		var date = new Date();
		var startTime = date.getTime();
		
		while(!finished) {
			var offset = this.model.get("offset");
			$.ajax({
				url: requestURL,
				type: "get",
				async: false,
				dataType: "json",
				data: {"execid": execId, "jobId": jobId, "ajax":"fetchExecJobLogs", "offset": offset, "length": 50000, "attempt": attempt},
				error: function(data) {
					console.log(data);
					finished = true;
				},
				success: function(data) {
					console.log("fetchLogs");
					if (data.error) {
						console.log(data.error);
						finished = true;
					}
					else if (data.length == 0) {
						finished = true;
					}
					else {
						var date = new Date();
						var endTime = date.getTime();
						if ((endTime - startTime) > 10000) {
							finished = true;
							showDialog("Alert","The log are taking a long time to finish loading. Azkaban has stopped loading them. Please click Refresh to restart the load.");
						} 
	
						var re = /(https?:\/\/(([-\w\.]+)+(:\d+)?(\/([\w/_\.]*(\?\S+)?)?)?))/g;
						var log = $("#logSection").text();
						if (!log) {
							log = data.data;
						}
						else {
							log += data.data;
						}
	
						var newOffset = data.offset + data.length;
	
						$("#logSection").text(log);
						log = $("#logSection").html();
						log = log.replace(re, "<a href=\"$1\" title=\"\">$1</a>");
						$("#logSection").html(log);
	
						model.set({"offset": newOffset, "log": log});
						$(".logViewer").scrollTop(9999);
					}
				}
			});
		}
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
