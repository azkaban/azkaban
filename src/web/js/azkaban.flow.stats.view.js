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

azkaban.FlowStatsModel = Backbone.Model.extend({});
azkaban.FlowStatsView = Backbone.View.extend({
  events: {
  },

	initialize: function(settings) {
		this.model.bind('change:view', this.handleChangeView, this);
		this.model.bind('render', this.render, this);
  },
	
  render: function(evt) {
  },

  show: function(execId) {
    this.analyzeExecution(execId);
  },

  fetchJobs: function(execId) {
    var requestURL = contextURL + "/executor";
    var requestData = {"execid": execId, "ajax":"fetchexecflow"};
    var jobs = [];
    var successHandler = function(data) {
      for (var i = 0; i < data.nodes.length; ++i) {
        var node = data.nodes[i];
        jobs.push(node.id);
      }
    };
    $.ajax({
      url: requestURL,
      data: requestData,
      success: successHandler,
      dataType: "json",
      async: false
    });
    return jobs;
  },

  fetchJobStats: function(jobId, execId) {
    var requestURL = contextURL + "/executor";
    var requestData = {
      "execid": execId,
      "flowid": flowId,
      "jobid": jobId,
      "ajax": "fetchExecJobStats"
    };
    var stats = null;
    var successHandler = function(data) {
      stats = data;
    };
    $.ajax({
      url: requestURL,
      data: requestData,
      success: successHandler,
      dataType: "json",
      async: false
    });
    return stats;
  },

  updateStats: function(jobStats, data) {
    var aggregateStats = data.stats;
    var state = jobStats.state;
    var conf = jobStats.conf;
    var mappers = parseInt(state.totalMappers);
    var reducers = parseInt(state.totalReducers);
    if (mappers > aggregateStats.maxMapSlots) {
      aggregateStats.maxMapSlots = mappers;
    }
    if (reducers > aggregateStats.maxReduceSlots) {
      aggregateStats.maxReduceSlots = reducers;
    }
    aggregateStats.totalMapSlots += mappers;
    aggregateStats.totalReduceSlots += reducers;
  },

  analyzeExecution: function(execId) {
    var jobs = this.fetchJobs(execId);
    if (jobs == null) {
      this.model.set({'data': null});
      this.model.trigger('render');
      return;
    }

    var data = {
      success: false,
      message: null,
      warnings: [],
      stats: {
        maxMapSlots: 0,
        maxReduceSlots: 0,
        totalMapSlots: 0,
        totalReduceSlots: 0,
        numJobs: jobs.length,
        longestTaskTime: 0
      }
    };

    for (var i = 0; i < jobs.length; ++i) {
      var job = jobs[i];
      var jobStats = this.fetchJobStats(job, execId);
      if (jobStats.jobStats == null) {
        data.warnings.push("No job stats available for job " + job.id);
        continue;
      }
      for (var j = 0; j < jobStats.jobStats.length; ++j) {
        this.updateStats(jobStats.jobStats[j], data);
      }
    }
    data.success = true;
    this.model.set({'data': data});
    this.model.trigger('render');
  },

	render: function(evt) {
    var view = this;
    var data = this.model.get('data');
    if (data == null) {
      var msg = { message: "Error retrieving flow stats."};
      dust.render("flowsummary-no-data", msg, function(err, out) {
        view.display(out);
      });
    }
    else if (data.success == "false") {
      dust.render("flowsummary-no-data", data, function(err, out) {
        view.display(out);
      });
    }
    else {
      dust.render("flowsummary-last-run", data, function(err, out) {
        view.display(out);
      });
    }
  },

  display: function(out) {
    $('#flow-stats-container').html(out);
  },
});
