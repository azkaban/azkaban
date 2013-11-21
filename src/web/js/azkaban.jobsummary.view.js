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
		this.handleUpdate();
	},
	handleUpdate: function(evt) {
		var requestURL = contextURL + "/executor"; 
		var model = this.model;
		var self = this;

		$.ajax({
			url: requestURL,
			dataType: "json",
			data: {"execid": execId, "jobId": jobId, "ajax":"fetchExecJobSummary", "attempt": attempt},
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
				for (var j = 0; j < headers.length; j++) {
					var td = document.createElement("td");
					$(td).text(row[j]);
					$(tr).append(td);
				}
				body.append(tr);
			}
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
