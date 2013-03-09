$.namespace('azkaban');

var messageDialogView;

azkaban.MessageDialogView = Backbone.View.extend({
  events : {
  },
  initialize : function(settings) {

  },
  show: function(title, message, callback) {
  	$("#azkabanMessageDialogTitle").text(title);
  	$("#azkabanMessageDialogText").text(message);
  	this.callback = callback;
  	
      $(this.el).modal({
          position: ["20%",],
          closeClass: "continueclass",
          containerId: 'confirm-container',
          containerCss: {
            'height': '220px',
            'width': '565px'
          },
          onShow: function (dialog) {
          },
          onClose: function() {
          	if (callback) {
          		callback.call();
          	}
          }
     });
  }
});


$(function() {
	messageDialogView = new azkaban.MessageDialogView({el: $('#azkabanMessageDialog')});
});
