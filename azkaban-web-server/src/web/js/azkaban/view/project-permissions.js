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

var permissionTableView;
var groupPermissionTableView;

azkaban.PermissionTableView = Backbone.View.extend({
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

var proxyTableView;
azkaban.ProxyTableView= Backbone.View.extend({
  events : {
    "click button": "handleRemoveProxy"
  },

  initialize : function(settings) {
  },

  render: function() {
  },

  handleRemoveProxy: function(evt) {
    removeProxyView.display($(evt.currentTarget).attr("name"));
  }
});

var removeProxyView;
azkaban.RemoveProxyView = Backbone.View.extend({
  events: {
    "click #remove-proxy-btn": "handleRemoveProxy"
  },

  initialize : function(settings) {
    $('#remove-proxy-error-msg').hide();
  },

  display: function(proxyName) {
    this.el.proxyName = proxyName;
    $("#remove-proxy-msg").text("Removing proxy user '" + proxyName + "'");
    $(this.el).modal().on('hide.bs.modal', function(e) {
      $('#remove-proxy-error-msg').hide();
    });
  },

  handleRemoveProxy: function() {
    var requestURL = contextURL + "/manager";
    var proxyName = this.el.proxyName;
    var requestData = {
      "project": projectName,
      "name": proxyName,
      "ajax": "removeProxyUser"
    };
    var successHandler = function(data) {
      console.log("Output");
      if (data.error) {
        $("#remove-proxy-error-msg").text(data.error);
        $("#remove-proxy-error-msg").slideDown();
        return;
      }
      var replaceURL = requestURL + "?project=" + projectName +"&permissions";
      window.location.replace(replaceURL);
    };

    $.get(requestURL, requestData, successHandler, "json");
  }
});

var addProxyView;
azkaban.AddProxyView = Backbone.View.extend({
  events: {
    "click #add-proxy-btn": "handleAddProxy"
  },

  initialize : function(settings) {
    $('#add-proxy-error-msg').hide();
  },

  display: function() {
    $(this.el).modal().on('hide.bs.modal', function(e) {
      $('#add-proxy-error-msg').hide();
    });
  },

  handleAddProxy: function() {
    var requestURL = contextURL + "/manager";
    var name = $('#proxy-user-box').val().trim();
    var requestData = {
      "project": projectName,
      "name": name,
      "ajax":"addProxyUser"
    };

    var successHandler = function(data) {
      console.log("Output");
      if (data.error) {
        $("#add-proxy-error-msg").text(data.error);
        $("#add-proxy-error-msg").slideDown();
        return;
      }

      var replaceURL = requestURL + "?project=" + projectName +"&permissions";
      window.location.replace(replaceURL);
    };
    $.get(requestURL, requestData, successHandler, "json");
  }
});

var changePermissionView;
azkaban.ChangePermissionView= Backbone.View.extend({
  events: {
    "click input[type=checkbox]": "handleCheckboxClick",
    "click #change-btn": "handleChangePermissions"
  },

  initialize: function(settings) {
    $('#change-permission-error-msg').hide();
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
    }
    else {
      if (group) {
        $('#change-title').text("Change Group Permissions");
      }
      else {
        $('#change-title').text("Change User Permissions");
      }

      $('#user-box').attr("disabled", "disabled");

      this.permission.admin = $(adminInput).is(":checked");
      this.permission.read = $(readInput).is(":checked");
      this.permission.write = $(writeInput).is(":checked");
      this.permission.execute = $(executeInput).is(":checked");
      this.permission.schedule = $(scheduleInput).is(":checked");
    }

    this.changeCheckbox();

    changePermissionView.render();
    $('#change-permission').modal().on('hide.bs.modal', function(e) {
      $('#change-permission-error-msg').hide();
    });
  },

  render: function() {
  },

  handleCheckboxClick: function(evt) {
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

  changeCheckbox: function(evt) {
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
      if (this.newPerm) {
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
    var name = $('#user-box').val().trim();
    var command = this.newPerm ? "addPermission" : "changePermission";
    var group = this.group;

    var permission = {};
    permission.admin = $("#admin-change").is(":checked");
    permission.read = $("#read-change").is(":checked");
    permission.write = $("#write-change").is(":checked");
    permission.execute = $("#execute-change").is(":checked");
    permission.schedule = $("#schedule-change").is(":checked");

    var requestData = {
      "project": projectName,
      "name": name,
      "ajax": command,
      "permissions": this.permission,
      "group": group
    };
    var successHandler = function(data) {
      console.log("Output");
      if (data.error) {
        $("#change-permission-error-msg").text(data.error);
        $("#change-permission-error-msg").slideDown();
        return;
      }

      var replaceURL = requestURL + "?project=" + projectName +"&permissions";
      window.location.replace(replaceURL);
    };

    $.get(requestURL, requestData, successHandler, "json");
  }
});

$(function() {
  permissionTableView = new azkaban.PermissionTableView({
    el: $('#permissions-table'),
    group: false,
    proxy: false
  });
  groupPermissionTableView = new azkaban.PermissionTableView({
    el: $('#group-permissions-table'),
    group: true,
    proxy: false
  });
  proxyTableView = new azkaban.ProxyTableView({
    el: $('#proxy-user-table'),
    group: false,
    proxy: true
  });
  changePermissionView = new azkaban.ChangePermissionView({
    el: $('#change-permission')
  });
  addProxyView = new azkaban.AddProxyView({
    el: $('#add-proxy')
  });
  removeProxyView = new azkaban.RemoveProxyView({
    el: $('#remove-proxy')
  });
  $('#addUser').bind('click', function() {
    changePermissionView.display("", true, false, false);
  });

  $('#addGroup').bind('click', function() {
    changePermissionView.display("", true, true, false);
  });

  $('#addProxyUser').bind('click', function() {
    addProxyView.display();
  });
});
