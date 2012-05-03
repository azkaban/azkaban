$.namespace('azkaban');

var navView;
azkaban.NavView = Backbone.View.extend({
	events : {
		"click #user-id":"handleUserMenu"
	},
	initialize : function(settings) {
		$("#user-menu").hide();
	},
	handleUserMenu : function(evt) {
		if ($("#user-menu").is(":visible")) {
			$("#user-menu").slideUp('fast');
		}
		else {
			$("#user-menu").slideDown('fast');
		}
	},
	render: function() {
	}
});

$(function() {
	navView = new azkaban.NavView({el:$( '#header' )});
});