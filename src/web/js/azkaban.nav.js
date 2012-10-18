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

(function($){
	$.getQuery = function( query ) {
		query = query.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
		var expr = "[\\?&]"+query+"=([^&#]*)";
		var regex = new RegExp( expr );
		var results = regex.exec( window.location.href );
		if( results !== null ) {
			return results[1];
			return decodeURIComponent(results[1].replace(/\+/g, " "));
		} else {
			return false;
		}
	};
})(jQuery);