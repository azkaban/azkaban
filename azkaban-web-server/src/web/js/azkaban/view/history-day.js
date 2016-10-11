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

var dayDataModel;
azkaban.DayDataModel = Backbone.Model.extend({});

var dayByDayView;
azkaban.DayByDayView = Backbone.View.extend({
  events: {
  },
  initialize: function(settings) {
    this.svgns = "http://www.w3.org/2000/svg";
    this.svg = $(this.el).find('svg')[0];
    this.columnDayWidth = 100;
    this.columnHourHeight = 50;
    this.columnHeight = 50*24;

    this.render(this);
  },
  prepareData: function(self) {
    var response = model.get("data");
    var start = data.start;
    var end = data.end;
    var data = data.data;

    var daysData = {};

    var startDate = new Date(start);

    while (startDate.getTime() < end) {
      daysData[startDate.getTime()] = new Array();
      startDate.setDate(startDate.getDate() + 1);
    }

    for (var i = 0; i < data.length; ++i) {
      var flow = data[i];

    }
  },
  render: function(self) {
    var svg = self.svg;
    var svgns = self.svgns;
    var width = $(svg).width();
    var height = $(svg).height();

    var mainG = document.createElementNS(this.svgns, 'g');
    $(svg).append(mainG);
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
  var requestURL = contextURL + "/history";

  var start = new Date();
  start.setHours(0);
  start.setMinutes(0);
  start.setSeconds(0);
  start.setMilliseconds(0);
  var end = new Date(start);

  start.setDate(start.getDate() - 7);
  console.log(start.getTime());

  end.setDate(end.getDate() + 1);
  console.log(end.getTime());

  dayDataModel = new azkaban.DayDataModel();
  dayByDayView = new azkaban.DayByDayView({el:$('#dayByDayPanel'), model: dayDataModel});

  $.get(
    requestURL,
    {"ajax":"fetch", "start": start.getTime(), "end": end.getTime()},
    function(data) {
      dayDataModel.set({data:data});
    },
    "json");
});
