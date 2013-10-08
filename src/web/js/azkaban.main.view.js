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

var projectTableView;
azkaban.ProjectTableView= Backbone.View.extend({
  events : {
    "click .project-expand": "expandProject"
  },
  initialize : function(settings) {

  },
  expandProject : function(evt) {
    if (evt.target.tagName!="SPAN") {
    	return;
    }
    
    var target = evt.currentTarget;
    var targetId = target.id;
    var requestURL = contextURL + "/manager";
    
    var targetExpanded = $('#' + targetId + '-child');
    var targetTBody = $('#' + targetId + '-tbody');
    
    var createFlowListFunction = this.createFlowListTable;
    
    if (target.loading) {
    	console.log("Still loading.");
    }
    else if (target.loaded) {
    	if($(targetExpanded).is(':visible')) {
    		$(target).addClass('expand').removeClass('collapse');
    		$(targetExpanded).fadeOut("fast");
    	}
    	else {
    	    $(target).addClass('collapse').removeClass('expand');
    		$(targetExpanded).fadeIn();
    	}
    }
    else {
	    // projectId is available
	    $(target).addClass('wait').removeClass('collapse').removeClass('expand');
	    target.loading = true;
	    
	    $.get(
	      requestURL,
	      {"project": targetId, "ajax":"fetchprojectflows"},
	      function(data) {
	        console.log("Success");
	        target.loaded = true;
	        target.loading = false;
	        
	        createFlowListFunction(data, targetTBody);
	        
			$(target).addClass('collapse').removeClass('wait');
	    	$(targetExpanded).fadeIn("fast");
	      },
	      "json"
	    );
    }
  },
  render: function() {
  },
  createFlowListTable : function(data, innerTable) {
  	var flows = data.flows;
  	flows.sort(function(a,b){return a.flowId.localeCompare(b.flowId);});
  	
  	var requestURL = contextURL + "/manager?project=" + data.project + "&flow=";
  	for (var i = 0; i < flows.length; ++i) {
  		var id = flows[i].flowId;
  		
  		var tr = document.createElement("tr");
		var idtd = document.createElement("td");
		$(idtd).addClass("tb-name");
  		
  		var ida = document.createElement("a");
		ida.project = data.project;
		$(ida).text(id);
		$(ida).attr("href", requestURL + id);
		
		$(idtd).append(ida);
		$(tr).append(idtd);
		$(innerTable).append(tr);
  	}
  }
});

var projectHeaderView;
azkaban.ProjectHeaderView= Backbone.View.extend({
  events : {
    "click #create-project-btn":"handleCreateProjectJob"
  },
  initialize : function(settings) {
    if (settings.errorMsg && settings.errorMsg != "null") {
      // Chrome bug in displaying placeholder text. Need to hide the box.
      $('#searchtextbox').hide();
      $('.messaging').addClass("error");
      $('.messaging').removeClass("success");
      $('.messaging').html(settings.errorMsg);
    }
    else if (settings.successMsg && settings.successMsg != "null") {
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

     console.log("Creating");
     $.ajax({
     	async: "false",
     	url: "manager",
     	dataType: "json",
     	type: "POST",
     	data: {action:"create", name:projectName, description:description},
     	success: function(data) {
     		if (data.status == "success") {
     			if (data.action == "redirect") {
     				window.location = data.path;
     			}
     		}
     		else {
     			if (data.action == "login") {
 					window.location = "";
     			}
     			else {
	     			$("#errorMsg").text("ERROR: " + data.message);
	    			$("#errorMsg").slideDown("fast");
    			}
     		}
     	}
     });

  },
  render: function() {
  }
});

var tableSorterView;
$(function() {
	projectHeaderView = new azkaban.ProjectHeaderView({el:$( '#all-jobs-content'), successMsg: successMessage, errorMsg: errorMessage });
	projectTableView = new azkaban.ProjectTableView({el:$('#all-jobs')});
	tableSorterView = new azkaban.TableSorter({el:$('#all-jobs'), initialSort: $('.tb-name')});
	uploadView = new azkaban.CreateProjectView({el:$('#create-project')});
});
