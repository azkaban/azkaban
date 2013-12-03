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
		
		while (!finished) {
			var offset = this.model.get("offset");
			var requestData = {
				"execid": execId, 
				"jobId": jobId, 
				"ajax":"fetchExecJobLogs", 
				"offset": offset, 
				"length": 50000, 
				"attempt": attempt
			};

			var successHandler = function(data) {
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
						showDialog("Alert","The log is taking a long time to finish loading. Azkaban has stopped loading them. Please click Refresh to restart the load.");
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

			$.ajax({
				url: requestURL,
				type: "get",
				async: false,
				data: requestData,
				dataType: "json",
				error: function(data) {
					console.log(data);
					finished = true;
				},
				success: successHandler
			});
		}
	}
});

var summaryModel;
azkaban.SummaryModel = Backbone.Model.extend({});

var jobSummaryView;
azkaban.JobSummaryView = Backbone.View.extend({
	events: {
		"click #updateSummaryBtn" : "handleUpdate"
	},
	initialize: function(settings) {
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var requestURL = contextURL + "/executor"; 
		var model = this.model;
		var self = this;

		var requestData = {
			"execid": execId, 
			"jobId": jobId, 
			"ajax":"fetchExecJobSummary", 
			"attempt": attempt
		};

		$.ajax({
			url: requestURL,
			dataType: "json",
			data: requestData,
			error: function(data) {
				console.log(data);
			},
			success: function(data) {
				console.log("fetchSummary");
				if (data.error) {
					console.log(data.error);
				}
				else {
					self.renderCommandTable(data.command, data.classpath, data.params);
					self.renderJobTable(data.summaryTableHeaders, data.summaryTableData, "summary");
					self.renderJobTable(data.statTableHeaders, data.statTableData, "stats");
				}
			}
		});
	},
	renderCommandTable: function(command, classpath, params) {
		if (command) {
			var commandTable = $("#commandTable");
			var i;
			
			// Add row for command
			var tr = document.createElement("tr");
			var td = document.createElement("td");
			$(td).append("<b>Command</b>");
			$(tr).append(td);
			td = document.createElement("td");
			$(td).text(command);
			$(tr).append(td);
			commandTable.append(tr);
			
			// Add row for classpath
			if (classpath && classpath.length > 0) {
				tr = document.createElement("tr");
				td = document.createElement("td");
				$(td).append("<b>Classpath</b>");
				$(tr).append(td);
				td = document.createElement("td");
				$(td).append(classpath[0]);
				for (i = 1; i < classpath.length; i++) {
					$(td).append("<br/>" + classpath[i]);
				}
				$(tr).append(td);
				commandTable.append(tr);
			}
			
			// Add row for params
			if (params && params.length > 0) {
				tr = document.createElement("tr");
				td = document.createElement("td");
				$(td).append("<b>Params</b>");
				$(tr).append(td);
				td = document.createElement("td");
				$(td).append(params[0]);
				for (i = 1; i < params.length; i++) {
					$(td).append("<br/>" + params[i]);
				}
				$(tr).append(td);
				commandTable.append(tr);
			}
		}
	},
	renderJobTable: function(headers, data, prefix) {
		if (headers) {
			// Add table headers
			var header = $("#" + prefix + "Header");
			var tr = document.createElement("tr");
			var i;
			for (i = 0; i < headers.length; i++) {
				var th = document.createElement("th");
				$(th).text(headers[i]);
				$(tr).append(th);
			}
			header.append(tr);
			
			// Add table body
			var body = $("#" + prefix + "Body");
			for (i = 0; i < data.length; i++) {
				tr = document.createElement("tr");
				var row = data[i];
				for (var j = 0; j < row.length; j++) {
					var td = document.createElement("td");
					if (j == 0) {
						// first column is a link to job details page 
						$(td).html(row[j]);
					} else {
						$(td).text(row[j]);
					}
					$(tr).append(td);
				}
				body.append(tr);
			}
		}
	}
});

var jobTabView;
azkaban.JobTabView = Backbone.View.extend({
	events: {
		'click #jobSummaryViewLink': 'handleJobSummaryViewLinkClick',
		'click #jobLogViewLink': 'handleJobLogViewLinkClick'
	},

	initialize: function(settings) {
		var selectedView = settings.selectedView;
		if (selectedView == 'joblog') {
			this.handleJobLogViewLinkClick();
		}
		else {
			this.handleJobSummaryViewLinkClick();
		}
	},

	render: function() {
	},

	handleJobLogViewLinkClick: function() {
		$('#jobSummaryViewLink').removeClass('selected');
		$('#jobSummaryView').hide();
		$('#jobLogViewLink').addClass('selected');
		$('#jobLogView').show();
	},
	
	handleJobSummaryViewLinkClick: function() {
		$('#jobSummaryViewLink').addClass('selected');
		$('#jobSummaryView').show();
		$('#jobLogViewLink').removeClass('selected');
		$('#jobLogView').hide();
	},
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
	jobLogView = new azkaban.JobLogView({
		el: $('#jobLogView'), 
		model: logModel
	});

	summaryModel = new azkaban.SummaryModel();
	jobSummaryView = new azkaban.JobSummaryView({
		el: $('#jobSummaryView'), 
		model: summaryModel
	});

	jobTabView = new azkaban.JobTabView({
		el: $('#headertabs')
	});

	if (window.location.hash) {
		var hash = window.location.hash;
		if (hash == '#joblog') {
			jobTabView.handleJobLogViewLinkClick();
		}
		else if (hash == '#jobsummary') {
			jobTabView.handleJobSummaryViewLinkClick();
		}
	}
});
