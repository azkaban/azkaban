$.namespace('azkaban');

var permissionTableView;
azkaban.PermissionTableView= Backbone.View.extend({
  events : {
	"click button": "handleChangePermission"
  },
  initialize : function(settings) {
  },
  render: function() {
  },
  handleChangePermission: function(evt) {
  	  var currentTarget = evt.currentTarget;
  	  changePermissionView.display(currentTarget.id, false);
  }
});


var changePermissionView;
azkaban.ChangePermissionView= Backbone.View.extend({
  events : {
  	"click input['checkbox']": "handleCheckboxClick",
  	"click #change-btn": "handleChangePermissions"
  },
  initialize : function(settings) {
  	$('#errorMsg').hide();
  },
  display: function(userid, newUser) {
  	this.userid = userid;
  	this.permission = {};
	$('#user-box').val(userid);
	this.newUser = newUser;
	
	var adminInput = $("#" + userid + "-admin-checkbox");
	var readInput = $("#" + userid + "-read-checkbox");
	var writeInput = $("#" + userid + "-write-checkbox");
	var executeInput = $("#" + userid + "-execute-checkbox");
	var scheduleInput = $("#" + userid + "-schedule-checkbox");
	
	if (newUser) {
		$('#change-title').text("Add New User");
		$('#user-box').attr("disabled", null);
		
		// default
		this.permission.admin = false;
		this.permission.read = true;
		this.permission.write = false;
		this.permission.execute = false;
		this.permission.schedule = false;
	}
	else {
		$('#change-title').text("Change Permissions");
		$('#user-box').attr("disabled", "disabled");
		
		this.permission.admin = $(adminInput).attr("checked");
		this.permission.read = $(readInput).attr("checked");
		this.permission.write = $(writeInput).attr("checked");
		this.permission.execute = $(executeInput).attr("checked");
		this.permission.schedule = $(scheduleInput).attr("checked");
	}
	
	this.changeCheckbox();
	
	changePermissionView.render();
  	 $('#change-permission').modal({
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
  },
  handleCheckboxClick : function(evt) {
  	console.log("click");
  	var targetName = evt.currentTarget.name;
  	this.permission[targetName] = evt.currentTarget.checked;
  	this.changeCheckbox(evt);
  },
  changeCheckbox : function(evt) {
    var perm = this.permission;
  	if (perm.admin) {
  		$("#admin-change").attr("checked", true);
  		$("#read-change").attr("checked", true);
  		$("#read-change").attr("disabled", "disabled");
  		
  		$("#write-change").attr("checked", true);
  		$("#write-change").attr("disabled", "disabled");

  		$("#execute-change").attr("checked", true);
  		$("#execute-change").attr("disabled", "disabled"); 
  		
  		$("#schedule-change").attr("checked", true);
  		$("#schedule-change").attr("disabled", "disabled");
  	}
  	else {
  		$("#admin-change").attr("checked", false);
  		$("#read-change").attr("checked", perm.read);
  		$("#read-change").attr("disabled", null);
  		  		
  		$("#write-change").attr("checked", perm.write);
  		$("#write-change").attr("disabled", null);
  		
  		$("#execute-change").attr("checked", perm.execute);
  		$("#execute-change").attr("disabled", null);
  		
  		$("#schedule-change").attr("checked", perm.schedule);
		$("#schedule-change").attr("disabled", null);
  	}
  	
  	$("#change-btn").removeClass("btn-disabled");
  	$("#change-btn").attr("disabled", null);
  	
  	if (perm.admin || perm.read || perm.write || perm.execute || perm.schedule) {
  		$("#change-btn").text("Commit");
  	}
  	else {
  		if(	this.newUser) {
  			$("#change-btn").disabled = true;
  			$("#change-btn").addClass("btn-disabled");
  		}
  		else {
  			$("#change-btn").text("Remove");
  		}
  	}
  },
  handleChangePermissions : function(evt) {
  	var requestURL = contextURL + "/manager";
  	var userID = $('#user-box').val();
	var command = this.newUser ? "addUserPermission" : "changeUserPermission";

  	$.get(
	      requestURL,
	      {"project": projectId, "username": userID, "json":command, "permissions": this.permission},
	      function(data) {
	      	  console.log("Output");
	      	  if (data.error) {
	      	  	$("#errorMsg").text(data.error);
	      	  	$("#errorMsg").show();
	      	  	return;
	      	  }
	      	  
	      	  var replaceURL = requestURL + "?project=" + projectId +"&permissions";
	          window.location.replace(replaceURL);
	      },
	      "json"
	    );
  }
});

$(function() {
	permissionTableView = new azkaban.PermissionTableView({el:$('#permissions-table')});
	changePermissionView = new azkaban.ChangePermissionView({el:$('#change-permission')});
	
	$('#addUser').bind('click', function() {
		changePermissionView.display("", true);
	});
});
