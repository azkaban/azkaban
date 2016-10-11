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

var messageDialogView;
azkaban.MessageDialogView = Backbone.View.extend({
  events: {
  },

  initialize: function(settings) {
  },

  show: function(title, message, callback) {
    $("#azkaban-message-dialog-title").text(title);
    $("#azkaban-message-dialog-text").text(message);
    this.callback = callback;
    $(this.el).on('hidden.bs.modal', function() {
      if (callback) {
        callback.call();
      }
    });
    $(this.el).modal();
  }
});

$(function() {
  messageDialogView = new azkaban.MessageDialogView({
    el: $('#azkaban-message-dialog')
  });
});
