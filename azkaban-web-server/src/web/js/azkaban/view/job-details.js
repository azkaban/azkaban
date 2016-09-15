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
  var jobLogModel = new azkaban.JobLogModel();
  jobLogView = new azkaban.JobLogView({
    el: $('#jobLogView'),
    model: jobLogModel
  });
  jobLogModel.refresh();
});
