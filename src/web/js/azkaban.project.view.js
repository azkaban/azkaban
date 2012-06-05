$.namespace('azkaban');

var projectView;
azkaban.ProjectView= Backbone.View.extend({
  events : {
      "click #project-upload-btn":"handleUploadProjectJob"
  },
  initialize : function(settings) {

  },
  handleUploadProjectJob : function(evt) {
      console.log("click upload project");
      $('#upload-project').modal({
          closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
          position: ["20%",],
          containerId: 'confirm-container',
          containerCss: {
            'height': '220px',
            'width': '565px'
          },
          onShow: function (dialog) {
            var modal = this;
            $("#errorMsg").hide();
          }
        });
  },
  render: function() {
  }
});

var uploadProjectView;
azkaban.UploadProjectView= Backbone.View.extend({
  events : {
    "click #upload-btn": "handleCreateProject"
  },
  initialize : function(settings) {
    $("#errorMsg").hide();
  },
  handleCreateProject : function(evt) {
	$("#upload-form").submit();
  },
  render: function() {
  }
});

$(function() {
	projectView = new azkaban.ProjectView({el:$( '#all-jobs-content'), successMsg: successMessage, errorMsg: errorMessage });
	uploadView = new azkaban.UploadProjectView({el:$('#upload-project')});
});
