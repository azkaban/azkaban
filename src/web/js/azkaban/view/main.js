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

var projectTableView;
azkaban.ProjectTableView = Backbone.View.extend({
  events: {
    "click .project-expander": "expandProject"
  },

  initialize: function(settings) {
  },

  expandProject: function(evt) {
    if (evt.target.tagName == "A") {
      return;
    }

    var target = evt.currentTarget;
    var targetId = target.id;
    var requestURL = contextURL + "/manager";

    var targetExpanded = $('#' + targetId + '-child');
    var targetTBody = $('#' + targetId + '-tbody');
    var createFlowListFunction = this.createFlowListTable;

    if (target.loading) {
      console.log("Still loading.");
    }
    else if (target.loaded) {
      if ($(targetExpanded).is(':visible')) {
        $(target).addClass('expanded').removeClass('collapsed');
        var expander = $(target).children('.project-expander-icon')[0];
        $(expander).removeClass('glyphicon-chevron-up');
        $(expander).addClass('glyphicon-chevron-down');
        $(targetExpanded).slideUp(300);
      }
      else {
        $(target).addClass('collapsed').removeClass('expanded');
        var expander = $(target).children('.project-expander-icon')[0];
        $(expander).removeClass('glyphicon-chevron-down');
        $(expander).addClass('glyphicon-chevron-up');
        $(targetExpanded).slideDown(300);
      }
    }
    else {
      // projectId is available
      $(target).addClass('wait').removeClass('collapsed').removeClass('expanded');
      target.loading = true;

      var request = {
        "project": targetId,
        "ajax": "fetchprojectflows"
      };

      var successHandler = function(data) {
        console.log("Success");
        target.loaded = true;
        target.loading = false;

        createFlowListFunction(data, targetTBody);

        $(target).addClass('collapsed').removeClass('wait');
        var expander = $(target).children('.project-expander-icon')[0];
        $(expander).removeClass('glyphicon-chevron-down');
        $(expander).addClass('glyphicon-chevron-up');
        $(targetExpanded).slideDown(300);
      };

      $.get(requestURL, request, successHandler, "json");
    }
  },

  render: function() {
  },

  createFlowListTable: function(data, innerTable) {
    var flows = data.flows;
    flows.sort(function(a,b) {
      return a.flowId.localeCompare(b.flowId);
    });
    var requestURL = contextURL + "/manager?project=" + data.project + "&flow=";
    for (var i = 0; i < flows.length; ++i) {
      var id = flows[i].flowId;
      var ida = document.createElement("a");
      ida.project = data.project;
      $(ida).text(id);
      $(ida).attr("href", requestURL + id);
      $(ida).addClass('list-group-item');
      $(innerTable).append(ida);
    }
  }
});

var projectHeaderView;
azkaban.ProjectHeaderView = Backbone.View.extend({
  events: {
    "click #create-project-btn": "handleCreateProjectJob"
  },

  initialize: function(settings) {
    console.log("project header view initialize.");
    if (settings.errorMsg && settings.errorMsg != "null") {
      $('#messaging').addClass("alert-danger");
      $('#messaging').removeClass("alert-success");
      $('#messaging-message').html(settings.errorMsg);
    }
    else if (settings.successMsg && settings.successMsg != "null") {
      $('#messaging').addClass("alert-success");
      $('#messaging').removeClass("alert-danger");
      $('#messaging-message').html(settings.successMsg);
    }
    else {
      $('#messaging').removeClass("alert-success");
      $('#messaging').removeClass("alert-danger");
    }
  },

  handleCreateProjectJob: function(evt) {
    $('#create-project-modal').modal();
  },

  render: function() {
  }
});

var createProjectView;
azkaban.CreateProjectView = Backbone.View.extend({
  events: {
    "click #create-btn": "handleCreateProject"
  },

  initialize: function(settings) {
    $("#modal-error-msg").hide();
  },

  handleCreateProject: function(evt) {
    // First make sure we can upload
    var projectName = $('#path').val();
    var description = $('#description').val();
    console.log("Creating");
    $.ajax({
      async: "false",
      url: "manager",
      dataType: "json",
      type: "POST",
      data: {
        action: "create",
        name: projectName,
        description: description
      },
      success: function(data) {
        if (data.status == "success") {
          if (data.action == "redirect") {
            window.location = data.path;
          }
        }
        else {
          if (data.action == "login") {
            window.location = "";
          }
          else {
            $("#modal-error-msg").text("ERROR: " + data.message);
            $("#modal-error-msg").slideDown("fast");
          }
        }
      }
    });
  },

  render: function() {
  }
});

var tableSorterView;
$(function() {
  projectHeaderView = new azkaban.ProjectHeaderView({
    el: $('#create-project'),
    successMsg: successMessage,
    errorMsg: errorMessage
  });

  projectTableView = new azkaban.ProjectTableView({
    el: $('#project-list')
  });

  /*tableSorterView = new azkaban.TableSorter({
    el: $('#all-jobs'),
    initialSort: $('.tb-name')
  });*/

  uploadView = new azkaban.CreateProjectView({
    el: $('#create-project-modal')
  });
});
