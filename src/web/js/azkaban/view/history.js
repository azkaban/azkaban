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

  initialize: function(settings) {
    $('#datetimebegin').datetimepicker();
    $('#datetimeend').datetimepicker();
    $('#datetimebegin').on('change.dp', function(e) {
      $('#datetimeend').data('DateTimePicker').setStartDate(e.date);
    });
    $('#datetimeend').on('change.dp', function(e) {
      $('#datetimebegin').data('DateTimePicker').setEndDate(e.date);
    });
    $('#adv-filter-error-msg').hide();
  },

  handleAdvFilter: function(evt) {
    console.log("handleAdv");
    var projcontain = $('#projcontain').val();
    var flowcontain = $('#flowcontain').val();
    var usercontain = $('#usercontain').val();
    var status = $('#status').val();
    var begin  = $('#datetimebegin').val();
    var end    = $('#datetimeend').val();

    console.log("filtering history");

    var historyURL = contextURL + "/history"
    var redirectURL = contextURL + "/schedule"

    var requestURL = historyURL + "?advfilter=true" + "&projcontain=" + projcontain + "&flowcontain=" + flowcontain + "&usercontain=" + usercontain + "&status=" + status + "&begin=" + begin + "&end=" + end ;
    window.location = requestURL;

    /*
    var requestData = {
      "action": "advfilter",
      "projre": projre,
      "flowre": flowre,
      "userre": userre
    };
    var successHandler = function(data) {
      if (data.action == "redirect") {
        window.location = data.redirect;
      }
    };
    $.get(historyURL, requestData, successHandler, "json");
  */
  },

  render: function() {
  }
});

$(function() {
  filterView = new azkaban.AdvFilterView({el: $('#adv-filter')});
  $('#adv-filter-btn').click( function() {
    $('#adv-filter').modal();
  });
});
