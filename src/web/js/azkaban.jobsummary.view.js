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

var summaryModel;
azkaban.SummaryModel = Backbone.Model.extend({});

var jobSummaryView;
azkaban.JobSummaryView = Backbone.View.extend({
	events: {
		"click #updateSummaryBtn" : "handleUpdate"
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
				data: {"execid": execId, "jobId": jobId, "ajax":"fetchExecJobSummary", "offset": offset, "length": 50000, "attempt": attempt},
				error: function(data) {
					console.log(data);
					finished = true;
				},
				success: function(data) {
					console.log("fetchSummary");
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
							showDialog("Alert","The summary is taking a long time to finish loading. Azkaban has stopped loading them. Please click Refresh to restart the load.");
						} 
	
						var re = /(https?:\/\/(([-\w\.]+)+(:\d+)?(\/([\w/_\.]*(\?\S+)?)?)?))/g;
						var summary = $("#summarySection").text();
						if (!summary) {
							summary = data.data;
						}
						else {
							summary += data.data;
						}
	
						var newOffset = data.offset + data.length;
	
						$("#summarySection").text(summary);
						summary = $("#summarySection").html();
						summary = summary.replace(re, "<a href=\"$1\" title=\"\">$1</a>");
						$("#summarySection").html(summary);
	
						model.set({"offset": newOffset, "summary": summary});
						$(".summaryViewer").scrollTop(9999);
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

	summaryModel = new azkaban.SummaryModel();
	jobSummaryView = new azkaban.JobSummaryView({el:$('#jobSummaryView'), model: summaryModel});
});
