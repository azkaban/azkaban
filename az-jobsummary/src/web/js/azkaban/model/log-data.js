/*
 * Copyright 2014 LinkedIn Corp.
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

azkaban.LogDataModel = Backbone.Model.extend({
  TIMESTAMP_REGEX: /^.*? - /gm,

  JOB_TRACKER_URL_REGEX: /https?:\/\/[-\w\.]+(?::\d+)?\/[\w\/\.]*\?\S+(job_\d{12}_\d{4,})\S*/,

  // Command properties
  COMMAND_START: "Command: ",
  CLASSPATH_REGEX: /(?:-cp|-classpath)\s+(\S+)/g,
  ENVIRONMENT_VARIABLES_REGEX: /-D(\S+)/g,
  JVM_MEMORY_REGEX: /(-Xm\S+)/g,
  PIG_PARAMS_REGEX: /-param\s+(\S+)/g,

  JOB_TYPE_REGEX: /Building (\S+) job executor/,

  PIG_JOB_SUMMARY_START: "HadoopVersion",
  PIG_JOB_STATS_START: "Job Stats (time in seconds):",

  HIVE_PARSING_START: "Parsing command: ",
  HIVE_PARSING_END: "Parse Completed",
  HIVE_NUM_MAP_REDUCE_JOBS_STRING: "Total MapReduce jobs = ",
  HIVE_MAP_REDUCE_JOB_START: "Starting Job",
  HIVE_MAP_REDUCE_JOBS_SUMMARY: "MapReduce Jobs Launched:",
  HIVE_MAP_REDUCE_SUMMARY_REGEX: /Job (\d+):\s+Map: (\d+)\s+Reduce: (\d+)\s+(?:Cumulative CPU: (.+?))?\s+HDFS Read: (\d+)\s+HDFS Write: (\d+)/,

  JOB_ID_REGEX: /job_\d{12}_\d{4,}/,

  initialize: function() {
    this.set("offset", 0 );
    this.set("logData", "");
    this.on("change:logData", this.parseLogData);
  },

  refresh: function() {
    var requestURL = contextURL + "/executor";
    var finished = false;

    var date = new Date();
    var startTime = date.getTime();

    while (!finished) {
      var requestData = {
        "execid": execId,
        "jobId": jobId,
        "ajax":"fetchExecJobLogs",
        "offset": this.get("offset"),
        "length": 50000,
        "attempt": attempt
      };

      var self = this;

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

          self.set("offset", data.offset + data.length);
          self.set("logData", self.get("logData") + data.data);
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
  },

  parseLogData: function() {
    var data = this.get("logData").replace(this.TIMESTAMP_REGEX, "");
    var lines = data.split("\n");

    if (this.parseCommand(lines)) {
      this.parseJobType(lines);
      this.parseJobTrackerUrls(lines);

      var jobType = this.get("jobType");
      if (jobType) {
        if (jobType.indexOf("pig") !== -1) {
          this.parsePigTable(lines, "pigSummary", this.PIG_JOB_SUMMARY_START, "", 0);
          this.parsePigTable(lines, "pigStats", this.PIG_JOB_STATS_START, "", 1);
        } else if (jobType.indexOf("hive") !== -1) {
          this.parseHiveQueries(lines);
        } else {
          this.parseJobIds(lines);
        }
      }
    }
  },

  parseCommand: function(lines) {
    var commandStartIndex = -1;
    var numLines = lines.length;
    for (var i = 0; i < numLines; i++) {
      if (lines[i].indexOf(this.COMMAND_START) === 0) {
        commandStartIndex = i;
        break;
      }
    }

    if (commandStartIndex != -1) {
      var commandProperties = {};

      var command = lines[commandStartIndex].substring(this.COMMAND_START.length);
      commandProperties.Command = command;

      this.parseCommandProperty(command, commandProperties, "Classpath", this.CLASSPATH_REGEX, ':');
      this.parseCommandProperty(command, commandProperties, "-D", this.ENVIRONMENT_VARIABLES_REGEX);
      this.parseCommandProperty(command, commandProperties, "Memory Settings", this.JVM_MEMORY_REGEX);
      this.parseCommandProperty(command, commandProperties, "Params", this.PIG_PARAMS_REGEX);

      this.set("commandProperties", commandProperties);

      return true;
    }

    return false;
  },

  parseCommandProperty: function(command, commandProperties, propertyName, regex, split) {
    var results = [];
    var match;
    while (match = regex.exec(command)) {
      if (split) {
        results = results.concat(match[1].split(split));
      } else {
        results.push(match[1]);
      }
    }

    if (results.length > 0) {
      commandProperties[propertyName] = results;
    }
  },

  parseJobTrackerUrls: function(lines) {
    var jobTrackerUrls = {};
    var jobTrackerUrlsOrdered = [];
    var numLines = lines.length;
    var match;
    for (var i = 0; i < numLines; i++) {
      if ((match = this.JOB_TRACKER_URL_REGEX.exec(lines[i])) && !jobTrackerUrls[match[1]]) {
        jobTrackerUrls[match[1]] = match[0];
        jobTrackerUrlsOrdered.push(match[0]);
      }
    }
    this.set("jobTrackerUrls", jobTrackerUrls);
    this.set("jobTrackerUrlsOrdered", jobTrackerUrlsOrdered);
  },

  parseJobIds: function(lines) {
    var seenJobIds = {};
    var jobIds = [];
    var numLines = lines.length;
    var match;
    for (var i = 0; i < numLines; i++) {
      if ((match = this.JOB_ID_REGEX.exec(lines[i])) && !seenJobIds[match[0]]) {
        seenJobIds[match[0]] = true;
        jobIds.push(match[0]);
      }
    }

    if (jobIds.length > 0) {
      this.set("jobIds", jobIds);
    }
  },

  parseJobType: function(lines) {
    var numLines = lines.length;
    var match;
    for (var i = 0; i < numLines; i++) {
      if (match = this.JOB_TYPE_REGEX.exec(lines[i])) {
        this.set("jobType", match[1]);
        break;
      }
    }
  },

  parsePigTable: function(lines, tableName, startPattern, endPattern, linesToSkipAfterStart) {
    var index = -1;
    var numLines = lines.length;
    for (var i = 0; i < numLines; i++) {
      if (lines[i].indexOf(startPattern) === 0) {
        index = i + linesToSkipAfterStart;
        break;
      }
    }

    if (index != -1) {
      var table = [];
      var line;
      while ((line = lines[index]) !== endPattern) {
        var columns = line.split("\t");
        // If first column is a job id, make it a link to the job tracker.
        if (this.get("jobTrackerUrls")[columns[0]]) {
          columns[0] = "<a href='" + this.get("jobTrackerUrls")[columns[0]] + "'>" + columns[0] + "</a>";
        }
        table.push(columns);
        index++;
      }

      this.set(tableName, table);
    }
  },

  parseHiveQueries: function(lines) {
    var hiveQueries = [];
    var hiveQueryJobs = [];

    var currMapReduceJob = 0;
    var numLines = lines.length;
    for (var i = 0; i < numLines;) {
      var line = lines[i];
      var parsingCommandIndex = line.indexOf(this.HIVE_PARSING_START);
      if (parsingCommandIndex === -1) {
        i++;
        continue;
      }

      // parse query text, which could span multiple lines
      var queryStartIndex = parsingCommandIndex + this.HIVE_PARSING_START.length;
      var query = line.substring(queryStartIndex) + "<br/>";

      i++;
      while (i < numLines && (line = lines[i]).indexOf(this.HIVE_PARSING_END) === -1) {
        query += line + "<br/>";
        i++;
      }
      hiveQueries.push(query);
      i++;

      // parse the query's Map-Reduce jobs, if any.
      var numMRJobs = 0;
      while (i < numLines) {
        line = lines[i];
        if (line.indexOf(this.HIVE_NUM_MAP_REDUCE_JOBS_STRING) !== -1) {
          // query involves map reduce jobs
          var numMRJobs = parseInt(line.substring(this.HIVE_NUM_MAP_REDUCE_JOBS_STRING.length),10);
          i++;

          // get the map reduce jobs summary
          while (i < numLines) {
            line = lines[i];
            if (line.indexOf(this.HIVE_MAP_REDUCE_JOBS_SUMMARY) !== -1) {
              // job summary table found
              i++;

              var queryJobs = [];

              var previousJob = -1;
              var numJobsSeen = 0;
              while (numJobsSeen < numMRJobs && i < numLines) {
                line = lines[i];
                var match;
                if (match = this.HIVE_MAP_REDUCE_SUMMARY_REGEX.exec(line)) {
                  var currJob = parseInt(match[1], 10);
                  if (currJob === previousJob) {
                    i++;
                    continue;
                  }

                  var job = [];
                  job.push("<a href='" + this.get("jobTrackerUrlsOrdered")[currMapReduceJob++] + "'>" + currJob + "</a>");
                  job.push(match[2]);
                  job.push(match[3]);
                  if (match[4]) {
                    this.set("hasCumulativeCPU", true);
                    job.push(match[4]);
                  }
                  job.push(match[5]);
                  job.push(match[6]);

                  queryJobs.push(job);
                  previousJob = currJob;
                  numJobsSeen++;
                }
                i++;
              }

              if (numJobsSeen === numMRJobs) {
                hiveQueryJobs.push(queryJobs);
              }

              break;
            }
            i++;
          }
          break;
        }
        else if (line.indexOf(this.HIVE_PARSING_START) !== -1) {
          if (numMRJobs === 0) {
            hiveQueryJobs.push(null);
          }
          break;
        }
        i++;
      }
      continue;
    }

    if (hiveQueries.length > 0) {
      this.set("hiveSummary", {
        hiveQueries: hiveQueries,
        hiveQueryJobs: hiveQueryJobs
      });
    }
  }
});
