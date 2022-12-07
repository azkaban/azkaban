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

var advFilterView;
azkaban.AdvFilterView = Backbone.View.extend({
  events: {
    "click #filter-btn": "handleAdvFilter"
  },

  initialize: function (settings) {
    $('#nextexecbegin').datetimepicker();
    $('#nextexecend').datetimepicker();
    $('#nextexecbegin').on('change.dp', function (e) {
      $('#nextexecend').data('DateTimePicker').setStartDate(e.date);
    });
    $('#nextexecend').on('change.dp', function (e) {
      $('#nextexecbegin').data('DateTimePicker').setEndDate(e.date);
    });
    $('#adv-filter-error-msg').hide();
  },

  handleAdvFilter: function (evt) {
    console.log("handleAdv");
    var projcontain = $('#projcontain').val();
    var flowcontain = $('#flowcontain').val();
    var submitusercontain = $('#submitusercontain').val();
    var nextexecbegin = $('#nextexecbegin').val();
    var nextexecend = $('#nextexecend').val();

    console.log("filtering history");

    var scheduleURL = contextURL + "/schedule"

    var requestURL = scheduleURL + "?advfilter=true" + "&projcontain="
        + projcontain + "&flowcontain=" + flowcontain + "&submitusercontain="
        + submitusercontain + "&nextexecbegin=" + nextexecbegin + "&nextexecend=" + nextexecend;
    window.location = requestURL;
  },

  render: function () {
  }
});

var slaView;
var tableSorterView;

$(function () {
  slaView = new azkaban.ChangeSlaView({el: $('#sla-options')});
  tableSorterView = new azkaban.TableSorter({el: $('#scheduledFlowsTbl')});
  //var requestURL = contextURL + "/manager";

  // Set up the Flow options view. Create a new one every time :p
  //$('#addSlaBtn').click( function() {
  //  slaView.show();
  //});

  filterView = new azkaban.AdvFilterView({el: $('#adv-filter')});
  $('#adv-filter-btn').click(function () {
    $('#adv-filter').modal();
  });
});