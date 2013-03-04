$.namespace('azkaban');

var permissionTableView;
var groupPermissionTableView;
var proxyTableView;
azkaban.PermissionTableView= Backbone.View.extend({
  events : {
	"click button": "handleChangePermission"
  },
  initialize : function(settings) {
  	this.group = settings.group;
  	this.proxy = settings.proxy;
  },
  render: function() {
  },
  handleChangePermission: function(evt) {
  	  var currentTarget = evt.currentTarget;
  	  changePermissionView.display(currentTarget.id, false, this.group, this.proxy);
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
  display: function(userid, newPerm, group, proxy) {
  	// 6 is the length of the prefix "group-"
  	this.userid = group ? userid.substring(6, userid.length) : userid;
  	if(group == true) {
  		this.userid = userid.substring(6, userid.length)
  	} else if (proxy == true) {
  		this.userid = userid.substring(6, userid.length)
  	} else {
  		this.userid = userid
  	}
  	
  	this.permission = {};
	$('#user-box').val(this.userid);
	this.newPerm = newPerm;
	this.group = group;
	this.proxy = proxy;
	
	var prefix = userid;
	var adminInput = $("#" + prefix + "-admin-checkbox");
	var readInput = $("#" + prefix + "-read-checkbox");
	var writeInput = $("#" + prefix + "-write-checkbox");
	var executeInput = $("#" + prefix + "-execute-checkbox");
	var scheduleInput = $("#" + prefix + "-schedule-checkbox");
	var proxyInput = $("#" + prefix + "-proxy-checkbox");
	
	if (newPerm) {
		if (group) {
			$('#change-title').text("Add New Group Permissions");
		}
		else if(proxy){
			$('#change-title').text("Add New Proxy User Permissions");
		}
		else{
			$('#change-title').text("Add New User Permissions");
		}
		$('#user-box').attr("disabled", null);
		
		// default
		this.permission.admin = false;
		this.permission.read = true;
		this.permission.write = false;
		this.permission.execute = false;
		this.permission.schedule = false;
		this.doProxy = false;
		
	}
	else {
		if (group) {
			$('#change-title').text("Change Group Permissions");
		}
		else if(proxy){
			$('#change-title').text("Change Proxy User Permissions");
			this.doProxy = $(proxyInput).attr("checked");
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
		this.doProxy = $(proxyInput).attr("checked");
	}
	
	if(proxy) {
		document.getElementById("otherCheckBoxes").hidden=true;
		document.getElementById("proxyCheckBox").hidden=false;
	} else {
		document.getElementById("otherCheckBoxes").hidden=false;
		document.getElementById("proxyCheckBox").hidden=true;
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
  	if(targetName == "proxy") {
  		this.doProxy = evt.currentTarget.checked;
  	}
  	else {
  		this.permission[targetName] = evt.currentTarget.checked;
  	}
  	this.changeCheckbox(evt);
  },
  changeCheckbox : function(evt) {
    var perm = this.permission;
    var proxy = this.proxy;
    var doProxy = this.doProxy;

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
  		
  		$("#proxy-change").attr("checked", false);
		$("#proxy-change").attr("disabled", "disabled");
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
		
		$("#proxy-change").attr("checked", doProxy);
		$("#proxy-change").attr("disabled", null);
		
  	}
  	
  	$("#change-btn").removeClass("btn-disabled");
  	$("#change-btn").attr("disabled", null);
  	
  	if (perm.admin || perm.read || perm.write || perm.execute || perm.schedule || doProxy) {
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
	if(this.proxy) {
		command = "addProxyUser";
	}
	var group = this.group;
	
  	$.get(
	      requestURL,
	      {"project": projectName, "name": name, "ajax":command, "permissions": this.permission, "doProxy": this.doProxy, "group": group},
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
	permissionTableView = new azkaban.PermissionTableView({el:$('#permissions-table'), group: false, proxy: false});
	groupPermissionTableView = new azkaban.PermissionTableView({el:$('#group-permissions-table'), group: true, proxy: false});
	proxyTableView = new azkaban.PermissionTableView({el:$('#proxy-user-table'), group: false, proxy: true});
	changePermissionView = new azkaban.ChangePermissionView({el:$('#change-permission')});
	
	$('#addUser').bind('click', function() {
		changePermissionView.display("", true, false, false);
	});
	
	$('#addGroup').bind('click', function() {
		changePermissionView.display("", true, true, false);
	});
	
	$('#addProxyUser').bind('click', function() {
		changePermissionView.display("", true, false, true);
	});
	
});
