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

var dbUploadPanel;
azkaban.DBUploadPanel= Backbone.View.extend({
  events : {
    "click #upload-jar-btn" : "handleUpload"
  },
  initialize : function(settings) {
  },
  render: function() {
  },
  handleUpload: function(){
    var filename = $("#file").val();
    if (filename.length > 4) {
      var lastIndexOf = filename.lastIndexOf('.');
      var lastIndexOfForwardSlash = filename.lastIndexOf('\\');
      var lastIndexOfBackwardSlash = filename.lastIndexOf('/');

      var startIndex = Math.max(lastIndexOfForwardSlash, lastIndexOfBackwardSlash);
      startIndex += 1;

      var subfilename = filename.substring(startIndex, filename.length);
      var end = filename.substring(lastIndexOf, filename.length);
      if (end != ".jar") {
        alert("File "+ subfilename + " doesn't appear to be a jar. Looking for mysql-connector*.jar");
        return;
      }
      else if (subfilename.substr(0, "mysql-connector".length) != "mysql-connector") {
        alert("File "+ subfilename + " doesn't appear to be a mysql connector jar. Looking for mysql-connector*.jar");
        return;
      }

      console.log("Looks valid, uploading.");
      var uploadForm = document.getElementById("upload-form");
      var formData = new FormData(uploadForm);
      var contextUrl = contextURL;

      var xhr = new XMLHttpRequest();
      xhr.onreadystatechange=function() {
        if (xhr.readyState==4) {
          var data = JSON.parse(xhr.responseText);
          if (data.error) {
            alert(data.error);
          }
          else {
            $("#installed").html("Uploaded <span class=bold>" + data.jarname + "</span>");
          }
        }
      }
      xhr.open("POST", "uploadServlet");
      xhr.send(formData);

      console.log("Finished.");
    }
    else {
      alert("File doesn't appear to be valid.");
    }
  }
});

var dbConnectionsPanel;
azkaban.DBConnectionPanel= Backbone.View.extend({
  events : {
    "click #save-connection-button" : "handleSaveConnection"
  },
  initialize : function(settings) {
    if (verified) {
      $("#save-results").text(message);
      $("#save-results").css("color", "#00CC00");
    } else {
      $("#save-results").hide();
    }
  },
  render: function() {
  },
  handleSaveConnection: function(){
    var host = $("#host").val();
    var port = $("#port").val();
    var database = $("#database").val();
    var username = $("#username").val();
    var password = $("#password").val();

    var contextUrl = contextURL;
    $.post(
      contextUrl,
      {
        ajax: "saveDbConnection",
        host: host,
        port: port,
        database: database,
        username: username,
        password: password
      },
      function(data) {
        if (data.error) {
          verified = false;
          $("#save-results").text(data.error);
          $("#save-results").css("color", "#FF0000");
        }
        else if (data.success) {
          verified = true;
          $("#save-results").text(data.success);
          $("#save-results").css("color", "#00CC00");
        }
        $("#save-results").show();
      }
    );
  }
});

$(function() {
  dbUploadPanel = new azkaban.DBUploadPanel({el:$( '#dbuploadpanel')});
  dbConnectionPanel = new azkaban.DBConnectionPanel({el:$( '#dbsettingspanel')});

  $("#saveAndContinue").click(function(data) {
    if (!verified) {
      alert("The database connection hasn't been verified.");
    }
    else {
      window.location="/?usersetup";
    }
  });
});
