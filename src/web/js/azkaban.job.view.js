$.namespace('azkaban');

var jobView;
azkaban.JobView= Backbone.View.extend({
  events : {
    "click #upload-btn":"handleUploadJob"
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
  handleUploadJob : function(evt) {
    console.log("click upload");
      $('#upload-job').modal({
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

var uploadView;
azkaban.UploadJobView= Backbone.View.extend({
  events : {
    "change #file": "handleFileChange",
    "click #deploy-btn": "handleUploadJob"
  },
	initialize : function(settings) {
    $("#errorMsg").hide();
	},
	handleUploadJob : function(evt) {
	  // First make sure we can upload

	  var projectName = $('#path').val();
	  var dir = document.getElementById('file').value;
	  if (projectName == "") {
	    $("#errorMsg").text("ERROR: Empty Project Name.");
	    $("#errorMsg").slideDown("fast");
	  }
	  else if (dir == "") {
	    $("#errorMsg").text("ERROR: No zip file selected.");
      $("#errorMsg").slideDown("fast");
	  }
	  else {
	     $("#deployform").submit();
  	}
	},
	handleFileChange : function(evt) {
		var path = $('#path');
		if(path.val() == '') {
			var dir = document.getElementById('file').value;
			var lastIndexOf = dir.lastIndexOf('.');
			var lastIndexOfForwardSlash = dir.lastIndexOf('\\');
			var lastIndexOfBackwardSlash = dir.lastIndexOf('/');
			
			var startIndex = Math.max(lastIndexOfForwardSlash, lastIndexOfBackwardSlash);
			startIndex += 1;
			path.val(dir.substring(startIndex, lastIndexOf));
		}
	},
	render: function() {
	}
});

$(function() {
	jobView = new azkaban.JobView({el:$( '#all-jobs-content'), successMsg: successMessage, errorMsg: errorMessage });
	uploadView = new azkaban.UploadJobView({el:$('#upload-job')});
});