$.namespace('azkaban');

var advFilterView;
azkaban.AdvFilterView = Backbone.View.extend({
	events: {
		"click #filter-btn": "handleAdvFilter"
	},
	initialize: function(settings) {
		$( "#datetimebegin" ).datetimepicker({
			dateFormat: "mm/dd/yy",
			separator: '-',
			timeFormat: "HH:mm"
		});
		$( "#datetimeend" ).datetimepicker({
			dateFormat: "mm/dd/yy",
			separator: '-',
			timeFormat: "HH:mm"
		});
		$("#errorMsg").hide();
	},
	handleAdvFilter: function(evt) {
		console.log("handleAdv");
		var projcontain = $('#projcontain').val();
		var flowcontain = $('#flowcontain').val();
		var usercontain = $('#usercontain').val();
		var status = $('#status').val();
		var begin  = $('#datetimebegin').val();
		var end    = $('#datetimeend').val();
		
		console.log("filtering history");

		var historyURL = contextURL + "/history"
		var redirectURL = contextURL + "/schedule"	
		

		var requestURL = historyURL + "?advfilter=true" + "&projcontain=" + projcontain + "&flowcontain=" + flowcontain + "&usercontain=" + usercontain + "&status=" + status + "&begin=" + begin + "&end=" + end ; 
		window.location = requestURL;
		
//		$.get(
//			historyURL,
//			{"action": "advfilter", "projre": projre, "flowre": flowre, "userre": userre},
//			function(data) {
//				if (data.action == "redirect") {
//                    window.location = data.redirect;
//                }
//			},
//			"json"
//		)
//		$.ajax({
//        	async: "false",
//        	url: "history",
//        	dataType: "json",
//        	type: "POST",
//        	data: {
//        	action:"advfilter",
//        	projre:projre,
//        	flowre:flowre,
//        	userre:userre
//        	},
//        	success: function(data) {
//        		if (data.redirect) {
//               		window.location = data.redirect;
//            	}
//        	}
//        })
	},
	render: function(){
	}
});

$(function() {

	filterView = new azkaban.AdvFilterView({el: $('#adv-filter')});

	 $('#adv-filter-btn').click( function() {
		$('#adv-filter').modal({
        closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
          position: ["20%",],
          containerId: 'confirm-container',
          containerCss: {
            'height': '220px',
            'width': '500px'
          },
          onShow: function (dialog) {
            var modal = this;
            $("#errorMsg").hide();
          }
        });
    });

});