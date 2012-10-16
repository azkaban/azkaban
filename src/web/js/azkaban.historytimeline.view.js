$.namespace('azkaban');

var timelineModel;
azkaban.TimelineModel = Backbone.Model.extend({});

azkaban.JobLogView = Backbone.View.extend({
	events: {
	},
	initialize: function(settings) {
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
	timelineModel = new azkaban.TimelineModel();
	jobLogView = new azkaban.JobLogView({el:$('#jobLogView'), model: timelineModel});
});
