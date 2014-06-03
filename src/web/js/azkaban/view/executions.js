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

var executionsTabView;
azkaban.ExecutionsTabView = Backbone.View.extend({
  events: {
    'click #currently-running-view-link': 'handleCurrentlyRunningViewLinkClick',
    'click #recently-finished-view-link': 'handleRecentlyFinishedViewLinkClick'
  },

  initialize: function(settings) {
    var selectedView = settings.selectedView;
    if (selectedView == 'recently-finished') {
      this.handleRecentlyFinishedViewLinkClick();
    }
    else {
      this.handleCurrentlyRunningViewLinkClick();
    }
  },

  render: function() {
  },

  handleCurrentlyRunningViewLinkClick: function() {
    $('#recently-finished-view-link').removeClass('active');
    $('#recently-finished-view').hide();
    $('#currently-running-view-link').addClass('active');
    $('#currently-running-view').show();
  },

  handleRecentlyFinishedViewLinkClick: function() {
    $('#currently-running-view-link').removeClass('active');
    $('#currently-running-view').hide();
    $('#recently-finished-view-link').addClass('active');
    $('#recently-finished-view').show();
  }
});

$(function() {
  executionsTabView = new azkaban.ExecutionsTabView({el: $('#header-tabs')});
  if (window.location.hash) {
    var hash = window.location.hash;
    if (hash == '#recently-finished') {
      executionsTabView.handleRecentlyFinishedLinkClick();
    }
  }
});
