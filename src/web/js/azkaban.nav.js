/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
