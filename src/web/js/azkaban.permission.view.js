$.namespace('azkaban');

var permissionTableView;
var groupPermissionTableView;
azkaban.PermissionTableView= Backbone.View.extend({
  events : {
	"click button": "handleChangePermission"
  },
  initialize : function(settings) {
  	this.group = settings.group;
  },
  render: function() {
  },
  handleChangePermission: function(evt) {
  	  var currentTarget = evt.currentTarget;
  	  changePermissionView.display(currentTarget.id, false, this.group);
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
  display: function(userid, newPerm, group) {
  	// 6 is the length of the prefix "group-"
  	this.userid = group ? userid.substring(6, userid.length) : userid;
  	this.permission = {};
	$('#user-box').val(this.userid);
	this.newPerm = newPerm;
	this.group = group;
	
	var prefix = userid;
	var adminInput = $("#" + prefix + "-admin-checkbox");
	var readInput = $("#" + prefix + "-read-checkbox");
	var writeInput = $("#" + prefix + "-write-checkbox");
	var executeInput = $("#" + prefix + "-execute-checkbox");
	var scheduleInput = $("#" + prefix + "-schedule-checkbox");
	
	if (newPerm) {
		if (group) {
			$('#change-title').text("Add New Group Permissions");
		}
		else {
			$('#change-title').text("Add New User Permissions");
		}
		$('#user-box').attr("disabled", null);
		
		// default
		this.permission.admin = false;
		this.permission.read = true;
		this.permission.write = false;
		this.permission.execute = false;
		this.permission.schedule = false;
	}
	else {
		if (group) {
			$('#change-title').text("Change Group Permissions");
		}
		else {
			$('#change-title').text("Change User Permissions");
		}
		
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
  		if(	this.newPerm) {
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
  	var name = $('#user-box').val();
	var command = this.newPerm ? "addPermission" : "changePermission";
	var group = this.group;
	
  	$.get(
	      requestURL,
	      {"project": projectName, "name": name, "ajax":command, "permissions": this.permission, "group": group},
	      function(data) {
	      	  console.log("Output");
	      	  if (data.error) {
	      	  	$("#errorMsg").text(data.error);
	      	  	$("#errorMsg").show();
	      	  	return;
	      	  }
	      	  
	      	  var replaceURL = requestURL + "?project=" + projectName +"&permissions";
	          window.location.replace(replaceURL);
	      },
	      "json"
	    );
  }
});

$(function() {
	permissionTableView = new azkaban.PermissionTableView({el:$('#permissions-table'), group: false});
	groupPermissionTableView = new azkaban.PermissionTableView({el:$('#group-permissions-table'), group: true});
	changePermissionView = new azkaban.ChangePermissionView({el:$('#change-permission')});
	
	$('#addUser').bind('click', function() {
		changePermissionView.display("", true, false);
	});
	
	$('#addGroup').bind('click', function() {
		changePermissionView.display("", true, true);
	});
});
