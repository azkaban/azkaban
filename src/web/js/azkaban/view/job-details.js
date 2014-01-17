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

var jobLogView;
azkaban.JobLogView = Backbone.View.extend({
	events: {
		"click #updateLogBtn" : "refresh"
	},

	initialize: function() {
		this.listenTo(this.model, "change:logData", this.render);
	},

	refresh: function() {
		this.model.refresh();
	},

	render: function() {
		var re = /(https?:\/\/(([-\w\.]+)+(:\d+)?(\/([\w/_\.]*(\?\S+)?)?)?))/g;
		var log = this.model.get("logData");
		log = log.replace(re, "<a href=\"$1\" title=\"\">$1</a>");
		$("#logSection").html(log);
	}
});

var jobSummaryView;
azkaban.JobSummaryView = Backbone.View.extend({
	events: {
		"click #updateSummaryBtn" : "refresh"
	},

	initialize: function(settings) {
		$("#jobType").hide();
		$("#commandSummary").hide();
		$("#pigJobSummary").hide();
		$("#pigJobStats").hide();
		$("#hiveJobSummary").hide();
		$("#jobIds").hide();

		this.listenTo(this.model, "change:jobType", this.renderJobTypeTable);
		this.listenTo(this.model, "change:commandProperties", this.renderCommandTable);
		this.listenTo(this.model, "change:pigSummary", this.renderPigSummaryTable);
		this.listenTo(this.model, "change:pigStats", this.renderPigStatsTable);
		this.listenTo(this.model, "change:hiveSummary", this.renderHiveTable);
		this.listenTo(this.model, "change:jobIds", this.renderJobIdsTable);
	},

	refresh: function() {
		this.model.refresh();
	},

	handleUpdate: function(evt) {
		renderJobTable(jobSummary.summaryTableHeaders, jobSummary.summaryTableData, "summary");
		renderJobTable(jobSummary.statTableHeaders, jobSummary.statTableData, "stats");
		renderHiveTable(jobSummary.hiveQueries, jobSummary.hiveQueryJobs);
	},

	renderJobTypeTable: function() {
		var jobTypeTable = $("#jobTypeTable");
		var jobType = this.model.get("jobType");

		var tr = document.createElement("tr");
		var td = document.createElement("td");
		$(td).html("<b>Job Type</b>");
		$(tr).append(td);
		td = document.createElement("td");
		$(td).html(jobType);
		$(tr).append(td);

		jobTypeTable.append(tr);

		$("#jobType").show();
	},

	renderJobIdsTable: function() {
		var oldBody = $("#jobIdsTableBody");
		var newBody = $(document.createElement("tbody")).attr("id", "jobIdsTableBody");

		var jobIds = this.model.get("jobIds");
		var jobUrls = this.model.get("jobTrackerUrls");
		var numJobs = jobIds.length;
		for (var i = 0; i < numJobs; i++) {
			var job = jobIds[i];
			var tr = document.createElement("tr");
			var td = document.createElement("td");
			var html = jobUrls[job] ? "<a href='" + jobUrls[job] + "'>" + job + "</a>" : job;
			$(td).html(html);
			$(tr).append(td);
			newBody.append(tr);
		}

		oldBody.replaceWith(newBody);

		$("#jobIds").show();
	},

	renderCommandTable: function() {
		var commandTable = $("#commandTable");
		var commandProperties = this.model.get("commandProperties");

		for (var key in commandProperties) {
			if (commandProperties.hasOwnProperty(key)) {
				var value = commandProperties[key];
				if (Array.isArray(value)) {
					value = value.join("<br/>");
				}
				var tr = document.createElement("tr");
				var keyTd = document.createElement("td");
				var valueTd = document.createElement("td");
				$(keyTd).html("<b>" + key + "</b>");
				$(valueTd).html(value);
				$(tr).append(keyTd);
				$(tr).append(valueTd);
				commandTable.append(tr);
			}
		}

		$("#commandSummary").show();
	},
	renderPigTable: function(tableName, data) {
		// Add table headers
		var header = $("#" + tableName + "Header");
		var tr = document.createElement("tr");
		var i;
		var headers = data[0];
		var numColumns = headers.length;
		for (i = 0; i < numColumns; i++) {
			var th = document.createElement("th");
			$(th).text(headers[i]);
			$(tr).append(th);
		}
		header.append(tr);
		
		// Add table body
		var body = $("#" + tableName + "Body");
		for (i = 1; i < data.length; i++) {
			tr = document.createElement("tr");
			var row = data[i];
			for (var j = 0; j < numColumns; j++) {
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

		$("#pigJob" + tableName.charAt(0).toUpperCase() + tableName.substring(1)).show();
	},
	renderPigSummaryTable: function() {
		this.renderPigTable("summary", this.model.get("pigSummary"));
	},
	renderPigStatsTable: function() {
		this.renderPigTable("stats", this.model.get("pigStats"));
	},
	renderHiveTable: function() {
		var hiveSummary = this.model.get("hiveSummary");
		var queries = hiveSummary.hiveQueries;
		var queryJobs = hiveSummary.hiveQueryJobs;

		// Set up table column headers
		var header = $("#hiveTableHeader");
		var tr = document.createElement("tr");

		var headers;
		if (this.model.get("hasCumulativeCPU")) {
			headers = ["Query","Job","Map","Reduce","Cumulative CPU","HDFS Read","HDFS Write"];
		} else {
			headers = ["Query","Job","Map","Reduce","HDFS Read","HDFS Write"];
		}

		var i;
		for (i = 0; i < headers.length; i++) {
			var th = document.createElement("th");
			$(th).text(headers[i]);
			$(tr).append(th);
		}
		header.html(tr);
		
		// Construct table body
		var oldBody = $("#hiveTableBody");
		var newBody = $(document.createElement("tbody")).attr("id", "hiveTableBody");
		for (i = 0; i < queries.length; i++) {
			// new query
			tr = document.createElement("tr");
			var td = document.createElement("td");
			$(td).html("<b>" + queries[i] + "</b>");
			$(tr).append(td);
			
			var jobs = queryJobs[i];
			if (jobs != null) {
				// add first job for this query
				var jobValues = jobs[0];
				var j;
				for (j = 0; j < jobValues.length; j++) {
					td = document.createElement("td");
					$(td).html(jobValues[j]);
					$(tr).append(td);
				}
				newBody.append(tr);
				
				// add remaining jobs for this query
				for (j = 1; j < jobs.length; j++) {
					jobValues = jobs[j];
					tr = document.createElement("tr");
					
					// add empty cell for query column
					td = document.createElement("td");
					$(td).html("&nbsp;");
					$(tr).append(td);
					
					// add job values
					for (var k = 0; k < jobValues.length; k++) {
						td = document.createElement("td");
						$(td).html(jobValues[k]);
						$(tr).append(td);
					}
					newBody.append(tr);
				}
				
			} else {
				newBody.append(tr);
			}
		}
		oldBody.replaceWith(newBody);

		$("#hiveJobSummary").show();
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
		if (selectedView == 'summary') {
			this.handleJobSummaryViewLinkClick();
		}
		else {
			this.handleJobLogViewLinkClick();
		}
	},

	handleJobLogViewLinkClick: function() {
		$('#jobSummaryViewLink').removeClass('active');
		$('#jobSummaryView').hide();
		$('#jobLogViewLink').addClass('active');
		$('#jobLogView').show();
	},
	
	handleJobSummaryViewLinkClick: function() {
		$('#jobSummaryViewLink').addClass('active');
		$('#jobSummaryView').show();
		$('#jobLogViewLink').removeClass('active');
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
	var logDataModel = new azkaban.LogDataModel();
	
	jobLogView = new azkaban.JobLogView({
		el: $('#jobLogView'), 
		model: logDataModel
	});

	jobSummaryView = new azkaban.JobSummaryView({
		el: $('#jobSummaryView'), 
		model: logDataModel
	});

	jobTabView = new azkaban.JobTabView({
		el: $('#headertabs')
	});

	logDataModel.refresh();

	if (window.location.hash) {
		var hash = window.location.hash;
		if (hash == '#logs') {
			jobTabView.handleJobLogViewLinkClick();
		}
		else if (hash == '#summary') {
			jobTabView.handleJobSummaryViewLinkClick();
		}
	}
});
