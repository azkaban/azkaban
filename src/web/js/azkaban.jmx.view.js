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

var jmxTableView;
azkaban.JMXTableView= Backbone.View.extend({
  events : {
    "click .querybtn": "queryJMX",
    "click .collapse": "collapseRow"
  },
  initialize : function(settings) {

  },
  queryJMX : function(evt) {
	var target = evt.currentTarget;
	var id = target.id;
	
	var childID = id + "-child";
	var tbody = id + "-tbody";
	
	var requestURL = contextURL + "/jmx";
	var canonicalName=$(target).attr("domain") + ":name=" + $(target).attr("name");

	var data = {"ajax":"getAllMBeanAttributes", "mBean":canonicalName};
	if ($(target).attr("hostPort")) {
		data.ajax = "getAllExecutorAttributes";
		data.hostPort = $(target).attr("hostPort");
	}
	$.get(
		requestURL,
		data,
		function(data) {
			var table = $('#' + tbody);
			$(table).empty();
			
			for(var key in data.attributes) {
				var value = data.attributes[key];
				
				var tr = document.createElement("tr");
				var tdName = document.createElement("td");
				var tdVal = document.createElement("td");
				
				$(tdName).text(key);
				$(tdVal).text(value);
				
				$(tr).append(tdName);
				$(tr).append(tdVal);
				
				$('#' + tbody).append(tr);
			}
			
			var child = $("#" + childID);
	    	$(child).fadeIn();
		}
	);
  },
  queryRemote : function(evt) {
	var target = evt.currentTarget;
	var id = target.id;
	
	var childID = id + "-child";
	var tbody = id + "-tbody";
	
	var requestURL = contextURL + "/jmx";
	var canonicalName=$(target).attr("domain") + ":name=" + $(target).attr("name");
	var hostPort = $(target).attr("hostport");
	$.get(
		requestURL,
		{"ajax":"getAllExecutorAttributes", "mBean":canonicalName, "hostPort": hostPort},
		function(data) {
			var table = $('#' + tbody);
			$(table).empty();
			
			for(var key in data.attributes) {
				var value = data.attributes[key];
				
				var tr = document.createElement("tr");
				var tdName = document.createElement("td");
				var tdVal = document.createElement("td");
				
				$(tdName).text(key);
				$(tdVal).text(value);
				
				$(tr).append(tdName);
				$(tr).append(tdVal);
				
				$('#' + tbody).append(tr);
			}
			
			var child = $("#" + childID);
	    	$(child).fadeIn();
		}
	);
  },
  collapseRow: function(evt) {
  	$(evt.currentTarget).parent().parent().fadeOut();
  },
  render: function() {
  }
});

var remoteTables = new Array();
$(function() {
	jmxTableView = new azkaban.JMXTableView({el:$('#all-jobs')});
	
	$(".remoteJMX").each(function(item) {
		var newTableView = new azkaban.JMXTableView({el:$(this)});
		remoteTables.push(newTableView);
	});
});
