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

var jobSummaryView;
azkaban.JobSummaryView = Backbone.View.extend({
  events: {
    "click #update-summary-btn" : "refresh"
  },

  initialize: function(settings) {
    $("#job-type").hide();
    $("#command-summary").hide();
    $("#pig-job-summary").hide();
    $("#pig-job-stats").hide();
    $("#hive-job-summary").hide();
    $("#job-ids").hide();

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
    var jobTypeTable = $("#job-type-table");
    var jobType = this.model.get("jobType");

    var tr = document.createElement("tr");
    var td = document.createElement("td");
    $(td).addClass("property-key");
    $(td).html("<b>Job Type</b>");
    $(tr).append(td);
    td = document.createElement("td");
    $(td).html(jobType);
    $(tr).append(td);

    jobTypeTable.append(tr);

    $("#placeholder").hide();
    $("#job-type").show();
  },

  renderJobIdsTable: function() {
    var oldBody = $("#job-ids-table-body");
    var newBody = $(document.createElement("tbody")).attr("id", "job-ids-table-body");

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

    $("#placeholder").hide();
    $("#job-ids").show();
  },

  renderCommandTable: function() {
    var commandTable = $("#command-table");
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

    $("#placeholder").hide();
    $("#command-summary").show();
  },

  renderPigTable: function(tableName, data) {
    // Add table headers
    var header = $("#" + tableName + "-header");
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
    var body = $("#" + tableName + "-body");
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

    $("#placeholder").hide();
    $("#pig-job-" + tableName).show();
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
    var header = $("#hive-table-header");
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
    var oldBody = $("#hive-table-body");
    var newBody = $(document.createElement("tbody")).attr("id", "hive-table-body");
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

    $("#placeholder").hide();
    $("#hive-job-summary").show();
  }
});

$(function() {
  var logDataModel = new azkaban.LogDataModel();
  jobSummaryView = new azkaban.JobSummaryView({
    el: $('#job-summary-view'),
    model: logDataModel
  });
  logDataModel.refresh();
});
