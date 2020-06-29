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

var initPagination = function (elem, model) {
  var totalPages = model.get("totalPages");
  if (!totalPages) {
    return;
  }

  // intercept TwbsPagination, because it only supports page >= 1
  // while azkaban job attempts start at 0
  var twbsPaginationPrototype =
      $.prototype.twbsPagination.prototype.constructor.Constructor.prototype;
  var originalBuildItem = twbsPaginationPrototype.buildItem;
  twbsPaginationPrototype.buildItem = function (type, page) {
    var $itemContainer = originalBuildItem.call(this, type, page);
    if (type == "page") {
      // apply page offset -1 as "attempt 0" == "page 1"
      $itemContainer.find('a').text(page - 1);
    }
    return $itemContainer;
  };

  $(elem).twbsPagination({
    totalPages: model.get("totalPages"),
    startPage: model.get("page"),
    initiateStartPageClick: false,
    visiblePages: model.get("visiblePages"),
    onPageClick: function (event, page) {
      model.set({"page": page});
    }
  });

};

var initAttemptPage = function (settings) {

  var jobLogModel = new azkaban.JobLogModel({
    page: settings.attempt + 1,
    totalPages: settings.pastAttempts + 1,
    visiblePages: 10,
    projectName: settings.projectName,
    flowId: settings.flowId
  });
  jobLogView = new azkaban.JobLogView({
    el: $('#jobLogView'),
    model: jobLogModel
  });
  jobLogModel.refresh();

}

var jobLogView;
azkaban.JobLogView = Backbone.View.extend({
  events: {
    "click #updateLogBtn": "refresh"
  },

  initialize: function () {
    this.handleInitView();
    this.listenTo(this.model, "change:logData", this.render);
    this.model.bind('change:page', this.handlePageChange, this);
  },

  refresh: function () {
    this.model.refresh();
  },

  render: function () {
    var re = /(https?:\/\/\S+)/g;
    var log = this.model.get("logData");
    log = log.replace(re, "<a href=\"$1\" title=\"\">$1</a>");
    $("#logSection").html(log);
  },

  handleInitView: function () {
    initPagination('#attemptsPagination', this.model);
  },

  handlePageChange: function () {
    var requestURL = contextURL + "/executor?project=" + projectName
        + "&execid=" + execId + "&job=" + jobId
        + "&attempt=" + (this.model.get("page") - 1);
    window.location.href = requestURL;
  },

});

var showDialog = function (title, message) {
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
