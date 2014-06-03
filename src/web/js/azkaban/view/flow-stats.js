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

  histogram: true,

  initialize: function(settings) {
    this.model.bind('change:view', this.handleChangeView, this);
    this.model.bind('render', this.render, this);
    if (settings.histogram != null) {
      this.histogram = settings.histogram;
    }
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
      data.nodes.sort(function(a, b) {
        return a.startTime - b.startTime;
      });
      jobs = data.nodes;
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

  updateStatsMapred: function(state, data, job) {
    var stats = data.stats;
    var mappers = parseInt(state.totalMappers);
    var reducers = parseInt(state.totalReducers);
    if (mappers >= stats.mapSlots.max) {
      stats.mapSlots.max = mappers;
      stats.mapSlots.job = job;
    }
    if (reducers >= stats.reduceSlots.max) {
      stats.reduceSlots.max = reducers;
      stats.reduceSlots.job = job;
    }
    stats.totalMapSlots += mappers;
    stats.totalReduceSlots += reducers;

  },

  updateStatsConf: function(conf, data, job) {
    var stats = data.stats;
    if (conf == null) {
      data.warnings.push("No job conf available for job " + job);
      return;
    }

    var jobMapMemoryMb = parseInt(conf['mapred.job.map.memory.mb']);
    if (jobMapMemoryMb >= stats.jobMapMemoryMb.max) {
      stats.jobMapMemoryMb.max = jobMapMemoryMb;
      stats.jobMapMemoryMb.job = job;
    }
    var jobReduceMemoryMb = parseInt(conf['mapred.job.reduce.memory.mb']);
    if (jobReduceMemoryMb >= stats.jobReduceMemoryMb.max) {
      stats.jobReduceMemoryMb.max = jobReduceMemoryMb;
      stats.jobReduceMemoryMb.job = job;
    }

    var childJavaOpts = conf['mapred.child.java.opts'];
    var parts = childJavaOpts.split(" ");
    for (var i = 0; i < parts.length; ++i) {
      var str = parts[i];
      if (str.indexOf('Xmx') > -1) {
        if (str.length <= 4) {
          continue;
        }
        var size = str.substring(4, str.length);
        var val = sizeStrToBytes(size);
        if (val >= stats.xmx.max) {
          stats.xmx.max = val;
          stats.xmx.str = size;
          stats.xmx.job = job;
        }
      }
      if (str.indexOf('Xms') > -1) {
        if (str.length <= 4) {
          continue;
        }
        var size = str.substring(4, str.length);
        var val = sizeStrToBytes(size);
        stats.xms.set = true;
        if (val >= stats.xms.max) {
          stats.xms.max = val;
          stats.xms.str = size;
          stats.xms.job = job;
        }
      }
    }

    var cacheFiles = conf['mapred.cache.files'];
    var cacheFilesFilesizes = conf['mapred.cache.files.filesizes'];
    if (cacheFiles != null && cacheFilesFilesizes != null) {
      stats.distributedCache.using = true;
      var parts = cacheFilesFilesizes.split(',');
      var size = 0;
      for (var i = 0; i < parts.length; ++i) {
        size += parseInt(parts[i]);
      }
      if (size >= stats.distributedCache.max) {
        stats.distributedCache.max = size;
        stats.distributedCache.job = job;
      }
    }
  },

  updateStatsCounters: function(state, data, job) {
    var stats = data.stats;
    if (state.counters == null) {
      data.warnings.push("No job counters available for job " + job);
      return;
    }
    var fileSystemCounters = state.counters['FileSystemCounters'];
    if (fileSystemCounters == null) {
      data.warnings.push("No FileSystemCounters available for job " + job);
      return;
    }
    var fileBytesRead = parseInt(fileSystemCounters['FILE_BYTES_READ']);
    if (fileBytesRead >= stats.fileBytesRead.max) {
      stats.fileBytesRead.max = fileBytesRead;
      stats.fileBytesRead.job = job;
    }

    var fileBytesWritten = parseInt(fileSystemCounters['FILE_BYTES_WRITTEN']);
    if (fileBytesWritten >= stats.fileBytesWritten.max) {
      stats.fileBytesWritten.max = fileBytesWritten;
      stats.fileBytesWritten.job = job;
    }

    var hdfsBytesRead = parseInt(fileSystemCounters['HDFS_BYTES_READ']);
    if (hdfsBytesRead >= stats.hdfsBytesRead.max) {
      stats.hdfsBytesRead.max = hdfsBytesRead;
      stats.hdfsBytesRead.job = job;
    }

    var hdfsBytesWritten = parseInt(fileSystemCounters['HDFS_BYTES_WRITTEN']);
    if (hdfsBytesWritten >= stats.hdfsBytesWritten.max) {
      stats.hdfsBytesWritten.max = hdfsBytesWritten;
      stats.hdfsBytesWritten.job = job;
    }
  },

  updateStats: function(jobStats, data, job) {
    var stats = data.stats;
    var state = jobStats.state;
    var conf = jobStats.conf;

    this.updateStatsMapred(state, data, job);
    this.updateStatsConf(conf, data, job);
    this.updateStatsCounters(state, data, job);
  },

  finalizeStats: function(data) {
    data.success = true;
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
      durations: [],
      histogram: this.histogram,
      stats: {
        mapSlots: {
          max: 0,
          job: null
        },
        reduceSlots: {
          max: 0,
          job: null
        },
        totalMapSlots: 0,
        totalReduceSlots: 0,
        numJobs: jobs.length,
        longestTaskTime: 0,
        jobMapMemoryMb: {
          max: 0,
          job: null
        },
        jobReduceMemoryMb: {
          max: 0,
          job: null
        },
        xmx: {
          max: 0,
          str: null,
          job: null
        },
        xms: {
          set: false,
          max: 0,
          str: null,
          job: null
        },
        fileBytesRead: {
          max: 0,
          job: null
        },
        hdfsBytesRead: {
          max: 0,
          job: null
        },
        fileBytesWritten: {
          max: 0,
          job: null
        },
        hdfsBytesWritten: {
          max: 0,
          job: null
        },
        distributedCache: {
          using: false,
          max: 0,
          job: null
        },
      }
    };

    var jobsAnalyzed = 0;
    for (var i = 0; i < jobs.length; ++i) {
      var job = jobs[i];
      var duration = job.endTime - job.startTime;
      data.durations.push({
        job: job.id,
        duration: duration
      });

      var jobStats = this.fetchJobStats(job.id, execId);
      if (jobStats.jobStats == null) {
        data.warnings.push("No job stats available for job " + job.id);
        continue;
      }
      for (var j = 0; j < jobStats.jobStats.length; ++j) {
        this.updateStats(jobStats.jobStats[j], data, job.id);
      }
      ++jobsAnalyzed;
    }

    // If no jobs were analyzed, then no jobs had any job stats available. In
    // this case, display a No Flow Stats Available message.
    if (jobsAnalyzed == 0) {
      data.success = false;
      data.message = "There were no job stats provided by any job.";
    }
    else {
      this.finalizeStats(data);
    }

    this.model.set({'data': data});
    this.model.trigger('render');
  },

  render: function(evt) {
    var view = this;
    var data = this.model.get('data');
    if (data == null) {
      var msg = { message: "Error retrieving flow stats."};
      dust.render("flowstats-no-data", msg, function(err, out) {
        view.display(out);
      });
    }
    else if (data.success == false) {
      dust.render("flowstats-no-data", data, function(err, out) {
        view.display(out);
      });
    }
    else {
      var histogram = this.histogram;
      dust.render("flowstats", data, function(err, out) {
        view.display(out);
        if (histogram == true) {
          var yLabelFormatCallback = function(y) {
            var seconds = y / 1000.0;
            return seconds.toString() + " s";
          };

          Morris.Bar({
            element: "job-histogram",
            data: data.durations,
            xkey: "job",
            ykeys: ["duration"],
            labels: ["Duration"],
            yLabelFormat: yLabelFormatCallback
          });
        }
      });
    }
  },

  display: function(out) {
    $('#flow-stats-container').html(out);
  },
});
