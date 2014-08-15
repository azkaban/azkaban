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

var projectView;
azkaban.ProjectView = Backbone.View.extend({
  events: {
    "click #project-upload-btn": "handleUploadProjectJob",
    "click #project-delete-btn": "handleDeleteProject"
  },

  initialize: function(settings) {
  },

  handleUploadProjectJob: function(evt) {
    console.log("click upload project");
    $('#upload-project-modal').modal();
  },

  handleDeleteProject: function(evt) {
    console.log("click delete project");
    $('#delete-project-modal').modal();
  },

  render: function() {
  }
});

var uploadProjectView;
azkaban.UploadProjectView = Backbone.View.extend({
  events: {
    "click #upload-project-btn": "handleCreateProject"
  },

  initialize: function(settings) {
    console.log("Hide upload project modal error msg");
    $("#upload-project-modal-error-msg").hide();
  },

  handleCreateProject: function(evt) {
    console.log("Upload project button.");
    $("#upload-project-form").submit();
  },

  render: function() {
  }
});

var deleteProjectView;
azkaban.DeleteProjectView = Backbone.View.extend({
  events: {
    "click #delete-btn": "handleDeleteProject"
  },

  initialize: function(settings) {
  },

  handleDeleteProject: function(evt) {
    $("#delete-form").submit();
  },

  render: function() {
  }
});

var projectDescription;
azkaban.ProjectDescriptionView = Backbone.View.extend({
  events: {
    "click #project-description": "handleDescriptionEdit",
    "click #project-description-btn": "handleDescriptionSave"
  },

  initialize: function(settings) {
    console.log("project description initialize");
  },

  handleDescriptionEdit: function(evt) {
    console.log("Edit description");
    var description = null;
    if ($('#project-description').hasClass('editable-placeholder')) {
      description = '';
      $('#project-description').removeClass('editable-placeholder');
    }
    else {
      description = $('#project-description').text();
    }
    $('#project-description-edit').attr("value", description);
    $('#project-description').hide();
    $('#project-description-form').show();
  },

  handleDescriptionSave: function(evt) {
    var newText = $('#project-description-edit').val();
    if ($('#project-description-edit').hasClass('has-error')) {
      $('#project-description-edit').removeClass('has-error');
    }
    var requestURL = contextURL + "/manager";
    var requestData = {
      "project": projectName,
      "ajax":"changeDescription",
      "description": newText
    };
    var successHandler = function(data) {
      if (data.error) {
        $('#project-description-edit').addClass('has-error');
        alert(data.error);
        return;
      }
      $('#project-description-form').hide();
      if (newText != '') {
        $('#project-description').text(newText);
      }
      else {
        $('#project-description').text('Add project description.');
        $('#project-description').addClass('editable-placeholder');
      }
      $('#project-description').show();
    };
    $.get(requestURL, requestData, successHandler, "json");
  },

  render: function() {
  }
});

$(function() {
  projectView = new azkaban.ProjectView({
    el: $('#project-options')
  });
  uploadView = new azkaban.UploadProjectView({
    el: $('#upload-project-modal')
  });
  deleteProjectView = new azkaban.DeleteProjectView({
    el: $('#delete-project-modal')
  });
  projectDescription = new azkaban.ProjectDescriptionView({
    el: $('#project-sidebar')
  });
});
