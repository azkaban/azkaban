$.namespace('azkaban');

var jobView;
azkaban.JobView= Backbone.View.extend({
  events : {
    "click #create-project-btn":"handleCreateProjectJob"
  },
  initialize : function(settings) {
    if (settings.errorMsg) {
      // Chrome bug in displaying placeholder text. Need to hide the box.
      $('#searchtextbox').hide();
      $('.messaging').addClass("error");
      $('.messaging').removeClass("success");
      $('.messaging').html(settings.errorMsg);
    }
    else if (settings.successMsg) {
      $('#searchtextbox').hide();
      $('.messaging').addClass("success");
      $('.messaging').removeClass("error");
      $('#message').html(settings.successMsg);
    }
    else {
      $('#searchtextbox').show();
      $('.messaging').removeClass("success");
      $('.messaging').removeClass("error");
    }
    
    $('#messageClose').click(function() {
      $('#searchtextbox').show();
      
      $('.messaging').slideUp('fast', function() {
        $('.messaging').removeClass("success");
        $('.messaging').removeClass("error");
      });
    });
  },
  handleCreateProjectJob : function(evt) {
    console.log("click create project");
      $('#create-project').modal({
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

var createProjectView;
azkaban.CreateProjectView= Backbone.View.extend({
  events : {
    "click #create-btn": "handleCreateProject"
  },
  initialize : function(settings) {
    $("#errorMsg").hide();
  },
  handleCreateProject : function(evt) {
	  // First make sure we can upload
	  var projectName = $('#path').val();
	  var description = $('#description').val();
	  if (projectName == "") {
	    $("#errorMsg").text("ERROR: Empty Project Name.");
	    $("#errorMsg").slideDown("fast");
	  }
	  else {
	     console.log("Deploying");
	     $.ajax({
	     	async: "false",
	     	url: "manager",
	     	dataType: "json",
	     	type: "POST",
	     	data: {action:"create", name:projectName, description:description},
	     	success: function(data) {console.log("success");}
	     });
	     	
	     
	    //window.location = "manager";
	     //$("#deployform").submit();
  	  }
  },
  render: function() {
  }
});

$(function() {
	jobView = new azkaban.JobView({el:$( '#all-jobs-content'), successMsg: successMessage, errorMsg: errorMessage });
	uploadView = new azkaban.CreateProjectView({el:$('#create-project')});
});
