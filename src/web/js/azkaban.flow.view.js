$.namespace('azkaban');

var handleJobMenuClick = function(action, el, pos) {
	var jobid = el[0].jobid;
	var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowName + "&job=" + jobid;
	if (action == "open") {
		window.location.href = requestURL;
	}
	else if(action == "openwindow") {
		window.open(requestURL);
	}
	else if(action == "disable") {
		var oldDisabled = graphModel.get("disabled");
		
		var newDisabled = {};
		// Copy disabled list
		if (oldDisabled) {
			for(var id in oldDisabled) {
			    if(oldDisabled.hasOwnProperty(id)) {
			    	var disabled = (oldDisabled[id]);
			    	if (disabled) {
			    		newDisabled[id]=true;
			    	}
			    }
			}
		}
		
		newDisabled[jobid] = true;
		graphModel.set({disabled: newDisabled});
	}
	else if(action == "enable") {
		var oldDisabled = graphModel.get("disabled");
		// Copy disabled list
		if (oldDisabled) {
			var newDisabled = {};
			for(var id in oldDisabled) {
			    if(oldDisabled.hasOwnProperty(id)) {
			    	var disabled = (oldDisabled[id]);
			    	if (disabled) {
			    		newDisabled[id]=true;
			    	}
			    }
			}
			
			if( oldDisabled[jobid]) {
				newDisabled[jobid] = false;
				graphModel.set({disabled: newDisabled});
			}
		}

	}
}

function hasClass(el, name) 
{
	var classes = el.getAttribute("class");
	if (classes == null) {
		return false;
	}
   return new RegExp('(\\s|^)'+name+'(\\s|$)').test(classes);
}

function addClass(el, name)
{
   if (!hasClass(el, name)) { 
   		var classes = el.getAttribute("class");
   		classes += classes ? ' ' + name : '' +name;
   		el.setAttribute("class", classes);
   }
}

function removeClass(el, name)
{
   if (hasClass(el, name)) {
      var classes = el.getAttribute("class");
      el.setAttribute("class", classes.replace(new RegExp('(\\s|^)'+name+'(\\s|$)'),' ').replace(/^\s+|\s+$/g, ''));
   }
}

var flowTabView;
azkaban.FlowTabView= Backbone.View.extend({
  events : {
  	"click #graphViewLink" : "handleGraphLinkClick",
  	"click #jobslistViewLink" : "handleJobslistLinkClick"
  },
  initialize : function(settings) {
  	var selectedView = settings.selectedView;
  	if (selectedView == "jobslist") {
  		this.handleJobslistLinkClick();
  	}
  	else {
  		this.handleGraphLinkClick();
  	}

  },
  render: function() {
  	console.log("render graph");
  },
  handleGraphLinkClick: function(){
  	$("#jobslistViewLink").removeClass("selected");
  	$("#graphViewLink").addClass("selected");
  	
  	$("#jobListView").hide();
  	$("#graphView").show();
  },
  handleJobslistLinkClick: function() {
  	$("#graphViewLink").removeClass("selected");
  	$("#jobslistViewLink").addClass("selected");
  	
  	 $("#graphView").hide();
  	 $("#jobListView").show();
  }
});

var jobListView;
azkaban.JobListView = Backbone.View.extend({
	events: {
		"keyup input": "filterJobs",
		"click li": "handleJobClick",
		"click #resetPanZoomBtn" : "handleResetPanZoom"
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.handleSelectionChange, this);
		this.model.bind('change:disabled', this.handleDisabledChange, this);
		this.model.bind('change:graph', this.render, this);
	},
	filterJobs: function(self) {
		var filter = $("#filter").val();
		
		if (filter && filter.trim() != "") {
			filter = filter.trim();
			
			if (filter == "") {
				if (this.filter) {
					$("#jobs").children().each(
						function(){
							var a = $(this).find("a");
        					$(a).html(this.jobid);
        					$(this).show();
						}
					);
				}
				
				this.filter = null;
				return;
			}
		}
		else {
			if (this.filter) {
				$("#jobs").children().each(
					function(){
						var a = $(this).find("a");
    					$(a).html(this.jobid);
    					$(this).show();
					}
				);
			}
				
			this.filter = null;
			return;
		}
		
		$("#jobs").children().each(
			function(){
        		var jobid = this.jobid;
        		var index = jobid.indexOf(filter);
        		if (index != -1) {
        			var a = $(this).find("a");
        			
        			var endIndex = index + filter.length;
        			var newHTML = jobid.substring(0, index) + "<span>" + jobid.substring(index, endIndex) + "</span>" + jobid.substring(endIndex, jobid.length);
        			
        			$(a).html(newHTML);
        			$(this).show();
        		}
        		else {
        			$(this).hide();
        		}
    	});
    	
    	this.filter = filter;
	},
	render: function(self) {
		var data = this.model.get("data");
		var nodes = data.nodes;
		var edges = data.edges;
		
		this.listNodes = {}; 
		if (nodes.length == 0) {
			console.log("No results");
			return;
		};
	
		var nodeArray = nodes.slice(0);
		nodeArray.sort(function(a,b){ 
			var diff = a.y - b.y;
			if (diff == 0) {
				return a.x - b.x;
			}
			else {
				return diff;
			}
		});
		
		var ul = document.createElement("ul");
		$(ul).attr("id", "jobs");
		for (var i = 0; i < nodeArray.length; ++i) {
			var li = document.createElement("li");
			var a = document.createElement("a");
			$(a).text(nodeArray[i].id);
			li.appendChild(a);
			ul.appendChild(li);
			li.jobid=nodeArray[i].id;
			
			$(li).contextMenu({
					menu: 'jobMenu'
				},
				handleJobMenuClick
			);
			
			this.listNodes[nodeArray[i].id] = li;
		}
		
		$("#list").append(ul);
	},
	handleJobClick : function(evt) {
		var jobid = evt.currentTarget.jobid;
		if(!evt.currentTarget.jobid) {
			return;
		}
		
		if (this.model.has("selected")) {
			var selected = this.model.get("selected");
			if (selected == jobid) {
				this.model.unset("selected");
			}
			else {
				this.model.set({"selected": jobid});
			}
		}
		else {
			this.model.set({"selected": jobid});
		}
	},
	handleDisabledChange: function(evt) {
		var disabledMap = this.model.get("disabled");
		for(var id in disabledMap) {
		    if(disabledMap.hasOwnProperty(id)) {
		    	var disabled = (disabledMap[id]);
		    	if (disabled) {
		    		$(this.listNodes[id]).addClass("nodedisabled");
		    	}
		    	else {
		    		$(this.listNodes[id]).removeClass("nodedisabled");
		    	}
		    }
		}
	},
	handleSelectionChange: function(evt) {
		if (!this.model.hasChanged("selected")) {
			return;
		}
		
		var previous = this.model.previous("selected");
		var current = this.model.get("selected");
		
		if (previous) {
			$(this.listNodes[previous]).removeClass("selected");
		}
		
		if (current) {
			$(this.listNodes[current]).addClass("selected");
		}
	},
	handleResetPanZoom: function(evt) {
		this.model.trigger("resetPanZoom");
	}
});

var svgGraphView;
azkaban.SvgGraphView = Backbone.View.extend({
	events: {
		"click g" : "clickGraph"
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.changeSelected, this);
		this.model.bind('change:disabled', this.handleDisabledChange, this);
		this.model.bind('change:graph', this.render, this);
		this.model.bind('resetPanZoom', this.resetPanZoom, this);
		
		this.svgns = "http://www.w3.org/2000/svg";
		this.xlinksn = "http://www.w3.org/1999/xlink";
		
		var graphDiv = this.el[0];
		var svg = $('#svgGraph')[0];
		this.svgGraph = svg;
		
		var gNode = document.createElementNS(this.svgns, 'g');
		gNode.setAttribute("id", "group");
		svg.appendChild(gNode);
		this.mainG = gNode;

		$(svg).svgNavigate();
	},
	initializeDefs: function(self) {
		var def = document.createElementNS(svgns, 'defs');
		def.setAttributeNS(null, "id", "buttonDefs");

		// ArrowHead
		var arrowHeadMarker = document.createElementNS(svgns, 'marker');
		arrowHeadMarker.setAttribute("id", "triangle");
		arrowHeadMarker.setAttribute("viewBox", "0 0 10 10");
		arrowHeadMarker.setAttribute("refX", "5");
		arrowHeadMarker.setAttribute("refY", "5");
		arrowHeadMarker.setAttribute("markerUnits", "strokeWidth");
		arrowHeadMarker.setAttribute("markerWidth", "4");
		arrowHeadMarker.setAttribute("markerHeight", "3");
		arrowHeadMarker.setAttribute("orient", "auto");
		var path = document.createElementNS(svgns, 'polyline');
		arrowHeadMarker.appendChild(path);
		path.setAttribute("points", "0,0 10,5 0,10 1,5");

		def.appendChild(arrowHeadMarker);
		
		this.svgGraph.appendChild(def);
	},
	render: function(self) {
		console.log("graph render");

		var data = this.model.get("data");
		var nodes = data.nodes;
		var edges = data.edges;
		if (nodes.length == 0) {
			console.log("No results");
			return;
		};
	
		// layout
		layoutGraph(nodes, edges);
		
		var bounds = {};
		this.nodes = {};
		for (var i = 0; i < nodes.length; ++i) {
			this.nodes[nodes[i].id] = nodes[i];
		}
		
		for (var i = 0; i < edges.length; ++i) {
			this.drawEdge(this, edges[i]);
		}
		
		for (var i = 0; i < nodes.length; ++i) {
			this.drawNode(this, nodes[i], bounds);
		}
		
		bounds.minX = bounds.minX ? bounds.minX - 200 : -200;
		bounds.minY = bounds.minY ? bounds.minY - 200 : -200;
		bounds.maxX = bounds.maxX ? bounds.maxX + 200 : 200;
		bounds.maxY = bounds.maxY ? bounds.maxY + 200 : 200;
		
		this.graphBounds = bounds;
		this.resetPanZoom();
	},
	changeSelected: function(self) {
		console.log("change selected");
		var selected = this.model.get("selected");
		var previous = this.model.previous("selected");
		
		if (previous) {
			// Unset previous
			var g = document.getElementById(previous);
			removeClass(g, "selected");
		}
		
		if (selected) {
			var g = document.getElementById(selected);
			var node = this.nodes[selected];
			
			addClass(g, "selected");
			
			var offset = 200;
			var widthHeight = offset*2;
			var x = node.x - offset;
			var y = node.y - offset;
			
			
			$("#svgGraph").svgNavigate("transformToBox", {x: x, y: y, width: widthHeight, height: widthHeight});
		}
	},
	clickGraph: function(self) {
		console.log("click");
		if (self.currentTarget.jobid) {
			this.model.set({"selected": self.currentTarget.jobid});
		}
	},
	drawEdge: function(self, edge) {
		var svg = self.svgGraph;
		var svgns = self.svgns;
		
		var startNode = this.nodes[edge.from];
		var endNode = this.nodes[edge.target];
		
		if (edge.guides) {
			var pointString = "" + startNode.x + "," + startNode.y + " ";

			for (var i = 0; i < edge.guides.length; ++i ) {
				edgeGuidePoint = edge.guides[i];
				pointString += edgeGuidePoint.x + "," + edgeGuidePoint.y + " ";
			}
			
			pointString += endNode.x + "," + endNode.y;
			var polyLine = document.createElementNS(svgns, "polyline");
			polyLine.setAttributeNS(null, "class", "edge");
			polyLine.setAttributeNS(null, "points", pointString);
			polyLine.setAttributeNS(null, "style", "fill:none;");
			self.mainG.appendChild(polyLine);
		}
		else { 
			var line = document.createElementNS(svgns, 'line');
			line.setAttributeNS(null, "class", "edge");
			line.setAttributeNS(null, "x1", startNode.x);
			line.setAttributeNS(null, "y1", startNode.y);
			line.setAttributeNS(null, "x2", endNode.x);
			line.setAttributeNS(null, "y2", endNode.y);
			
			self.mainG.appendChild(line);
		}
	},
	handleDisabledChange: function(evt) {
		var disabledMap = this.model.get("disabled");
		for(var id in disabledMap) {
		    if(disabledMap.hasOwnProperty(id)) {
		    	var disabled = disabledMap[id];
		    	this.nodes[id].disabled = disabled;
		    	var g = document.getElementById(id);
		    	
		    	if (disabled) {
		    		this.nodes[id].disabled = disabled;
					addClass(g, "disabled");
		    	}
		    	else {
		    		removeClass(g, "disabled");
		    	}
		    }
		}
	},
	drawNode: function(self, node, bounds) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var xOffset = 10;
		var yOffset = 10;

		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttributeNS(null, "class", "jobnode");
		nodeG.setAttributeNS(null, "id", node.id);
		nodeG.setAttributeNS(null, "font-family", "helvetica");
		nodeG.setAttributeNS(null, "transform", "translate(" + node.x + "," + node.y + ")");
		
		var innerG = document.createElementNS(svgns, "g");
		innerG.setAttributeNS(null, "transform", "translate(-10,-10)");
		
		var circle = document.createElementNS(svgns, 'circle');
		circle.setAttributeNS(null, "cy", 10);
		circle.setAttributeNS(null, "cx", 10);
		circle.setAttributeNS(null, "r", 12);
		circle.setAttributeNS(null, "style", "width:inherit;stroke-opacity:1");
		
		
		var text = document.createElementNS(svgns, 'text');
		var textLabel = document.createTextNode(node.label);
		text.appendChild(textLabel);
		text.setAttributeNS(null, "x", 4);
		text.setAttributeNS(null, "y", 15);
		text.setAttributeNS(null, "height", 10); 
				
		this.addBounds(bounds, {minX:node.x - xOffset, minY: node.y - yOffset, maxX: node.x + xOffset, maxY: node.y + yOffset});
		
		var backRect = document.createElementNS(svgns, 'rect');
		backRect.setAttributeNS(null, "x", 0);
		backRect.setAttributeNS(null, "y", 2);
		backRect.setAttributeNS(null, "class", "backboard");
		backRect.setAttributeNS(null, "width", 10);
		backRect.setAttributeNS(null, "height", 15);
		
		innerG.appendChild(circle);
		innerG.appendChild(backRect);
		innerG.appendChild(text);
		innerG.jobid = node.id;

		nodeG.appendChild(innerG);
		self.mainG.appendChild(nodeG);

		// Need to get text width after attaching to SVG.
		var computeText = text.getComputedTextLength();
		var halfWidth = computeText/2;
		text.setAttributeNS(null, "x", -halfWidth + 10);
		backRect.setAttributeNS(null, "x", -halfWidth);
		backRect.setAttributeNS(null, "width", computeText + 20);

		nodeG.setAttributeNS(null, "class", "node");
		nodeG.jobid=node.id;
		$(nodeG).contextMenu({
				menu: 'jobMenu'
			},
			handleJobMenuClick
		);
	},
	addBounds: function(toBounds, addBounds) {
		toBounds.minX = toBounds.minX ? Math.min(toBounds.minX, addBounds.minX) : addBounds.minX;
		toBounds.minY = toBounds.minY ? Math.min(toBounds.minY, addBounds.minY) : addBounds.minY;
		toBounds.maxX = toBounds.maxX ? Math.max(toBounds.maxX, addBounds.maxX) : addBounds.maxX;
		toBounds.maxY = toBounds.maxY ? Math.max(toBounds.maxY, addBounds.maxY) : addBounds.maxY;
	},
	resetPanZoom : function(self) {
		var bounds = this.graphBounds;
		$("#svgGraph").svgNavigate("transformToBox", {x: bounds.minX, y: bounds.minY, width: (bounds.maxX - bounds.minX), height: (bounds.maxY - bounds.minY) });
	}
});

var graphModel;
azkaban.GraphModel = Backbone.Model.extend({});

$(function() {
	var selected;
	
	if (window.location.hash) {
		var hash = window.location.hash;
		if (hash == "#jobslist") {
			selected = "jobslist";
		}
		else if (hash == "#graph") {
			// Redundant, but we may want to change the default. 
			selected = "graph";
		}
		else {
			selected = "graph";
		}
	}
	flowTabView = new azkaban.FlowTabView({el:$( '#headertabs'), selectedView: selected });

	graphModel = new azkaban.GraphModel();
	svgGraphView = new azkaban.SvgGraphView({el:$('#svgDiv'), model: graphModel});
	jobsListView = new azkaban.JobListView({el:$('#jobList'), model: graphModel});
	
	var requestURL = contextURL + "/manager";

	$.get(
	      requestURL,
	      {"project": projectName, "ajax":"fetchflowgraph", "flow":flowName},
	      function(data) {
	          console.log("data fetched");
	          graphModel.set({data: data});
	          graphModel.trigger("change:graph");
	      },
	      "json"
	    );
	    
	$("#executebtn").click( function() {
		var executeURL = contextURL + "/executor";
		$.get(
			executeURL,
			{"project": projectName, "ajax":"executeFlow", "flow":flowName, "disabled":graphModel.get("disabled")},
			function(data) {
				alert("call success");
			},
			"json"
		);
		
	});
});
