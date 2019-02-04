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

var jobHistoryView;

var dataModel;
azkaban.DataModel = Backbone.Model.extend({});

var initJobHistoryPage = function (settings) {
  dataModel = new azkaban.DataModel({
    page: settings.page,
    pageSize: settings.pageSize,
    visiblePages: 5,
    recordCount: settings.recordCount,
    dataSeries: settings.dataSeries,
    projectName: settings.projectName,
    jobId: settings.jobId,
    fetchJobHistoryUrl: settings.fetchJobHistoryUrl
  });

  dataModel.trigger('render');

  jobHistoryView = new azkaban.TimeGraphView({
    el: $('#timeGraph'),
    model: dataModel,
    modelField: "dataSeries"
  });

  if (settings.recordCount) {
    $('#jobHistoryPagination').twbsPagination({
      totalPages: Math.ceil(
          dataModel.get("recordCount") / dataModel.get("pageSize")),
      startPage: dataModel.get("page"),
      initiateStartPageClick: false,
      visiblePages: dataModel.get("visiblePages"),
      onPageClick: function (event, page) {
        var qparams = {
          "project": dataModel.get("projectName"),
          "job": dataModel.get("jobId"),
          "page": page,
          "size": dataModel.get("pageSize")
        };
        window.location.href = dataModel.get("fetchJobHistoryUrl") + "?history&"
            + $.param(qparams);
      }
    });
  }

};
