$.namespace('azkaban');

var logModel;
azkaban.LogModel = Backbone.Model.extend({});

var projectLogView;
azkaban.ProjectLogView = Backbone.View.extend({
	events: {
		"click #updateLogBtn" : "handleUpdate"
	},
	initialize: function(settings) {
		this.model.set({"current": 0});
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var current = this.model.get("current");
		var requestURL = contextURL + "/manager"; 
		var model = this.model;

		$.get(
			requestURL,
			{"project": projectName, "ajax":"fetchProjectLogs", "tail": 100000},
			function(data) {
			console.log("fetchLogs");
	          	if (data.error) {
	          		showDialog("Error", data.error);
	          	}
	          	else {
	          		$("#logSection").text(data);
	          		model.set({"log": data});
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
	projectLogView = new azkaban.ProjectLogView({el:$('#projectLogView'), model: logModel});
});
