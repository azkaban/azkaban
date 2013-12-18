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

var projectSummary;
azkaban.ProjectSummaryView = Backbone.View.extend({
	events: {
		"click #edit": "handleDescriptionEdit"
	},

	initialize: function(settings) {
	},
	
	handleDescriptionEdit: function(evt) {
		console.log("Edit description");
		var editText = $("#edit").text();
		var descriptionTD = $('#pdescription');
		
		if (editText != "Edit Description") {
			var requestURL = contextURL + "/manager";
			var newText = $("#descEdit").val();

			$.get(
				requestURL,
				{
					"project": projectName, 
					"ajax":"changeDescription", 
					"description":newText
				},
				function(data) {
					if (data.error) {
						alert(data.error);
					}
				},
				"json"
			);
				
			$(descriptionTD).remove("#descEdit");
			$(descriptionTD).text(newText);
			$("#edit").text("Edit Description");
		}
		else {
			var text = $(descriptionTD).text();
			var edit = document.createElement("textarea");
			
			$(edit).addClass("editTextArea");
			$(edit).attr("id", "descEdit");
			$(edit).val(text);
			$(descriptionTD).text("");
			$(descriptionTD).append(edit);
			
			$("#edit").text("Commit");
		}
	},
	render: function() {
	}
});

$(function() {
	projectView = new azkaban.ProjectView({el:$('#project-options')});
	uploadView = new azkaban.UploadProjectView({el:$('#upload-project-modal')});
	deleteProjectView = new azkaban.DeleteProjectView({el: $('#delete-project-modal')});
	projectSummary = new azkaban.ProjectSummaryView({el:$('#project-summary')});
});
