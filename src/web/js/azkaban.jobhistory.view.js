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

var jobHistoryView;
azkaban.JobHistoryView = Backbone.View.extend({
	events: {
	},
	initialize: function(settings) {
		this.render();
	},
	render : function(self) {
		var data = this.model.get("data");
	
		var margin = {top: 20, right: 20, bottom: 30, left: 70},
	    width = $(this.el).width() - margin.left - margin.right,
	    height = 300 - margin.top - margin.bottom;
	    
	    var x = d3.time.scale()
		    .range([0, width]);
		
		var y = d3.scale.linear()
		    .range([height, 0]);
	    
	    var xAxis = d3.svg.axis()
		    .scale(x)
		    .orient("bottom");

		var yAxis = d3.svg.axis()
		    .scale(y)
		    .orient("left");
		yAxis.tickFormat(
			function(d) {
				return formatDuration(d, 1);
			}
		);
		
		var line = d3.svg.line()
		    .x(function(d) { return x(d.startTime); })
		    .y(function(d) { return y(d.endTime - d.startTime); });
		 
		var svg = d3.select("#timeGraph").append("svg")
		    .attr("width", width + margin.left + margin.right)
		    .attr("height", height + margin.top + margin.bottom)
		    .append("g")
		    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
		  
		  var xextent = d3.extent(data, function(d) { return d.startTime; });
		  var diff = (xextent[1] - xextent[0])*0.05;
		  
		  xextent[0] -= diff;
		  xextent[1] += diff;
		  x.domain(xextent);
		  
		  var yextent = d3.extent(data, function(d) { return d.endTime - d.startTime; });
		  var upperYbound = yextent[1]*1.25;
		  y.domain([0, upperYbound]);
		
		  svg.append("g")
		      .attr("class", "x axis")
		      .attr("transform", "translate(0," + height + ")")
		      .call(xAxis);
		
		  svg.append("g")
		      .attr("class", "y axis")
		      .call(yAxis)
		      .append("text")
		      .attr("transform", "rotate(-90)")
		      .attr("y", 6)
		      .attr("dy", ".71em")
		      .style("text-anchor", "end")
		      .text("Duration");
		
		  svg.append("path")
		      .datum(data)
		      .attr("class", "line")
		      .attr("d", line);
		      
		  var node = svg.selectAll("g.node")
		  				.data(data)
		  				.attr("class", "node")
		  				.enter().append("g")
		  				.attr("transform",  function(d) { return "translate(" + x(d.startTime) + "," + y(d.endTime-d.startTime) + ")";});

		  
		  node.append("circle")
		  	  .attr("r", 5)
		  	  .attr("class", function(d) {return d.status;})
		  	  .append("svg:title")
		  		.text(function(d) { return d.execId + ":" + d.flowId + " ran in " + getDuration(d.startTime, d.endTime)});
	}
});

var dataModel;
azkaban.DataModel = Backbone.Model.extend({});

$(function() {
	var selected;
	var series = dataSeries;
	dataModel = new azkaban.DataModel();
	dataModel.set({"data":series});
	jobDurationView = new azkaban.JobHistoryView({el:$('#timeGraph'), model: dataModel});
});
