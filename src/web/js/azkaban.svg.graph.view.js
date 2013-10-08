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


azkaban.SvgGraphView = Backbone.View.extend({
	events: {
		"click g" : "clickGraph",
		"contextmenu" : "handleRightClick",
		"contextmenu g" : "handleRightClick",
		"contextmenu polyline": "handleRightClick"
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.changeSelected, this);
		this.model.bind('change:graph', this.render, this);
		this.model.bind('resetPanZoom', this.resetPanZoom, this);
		this.model.bind('change:update', this.handleStatusUpdate, this);
		this.model.bind('change:disabled', this.handleDisabledChange, this);
		this.model.bind('change:updateAll', this.handleUpdateAllStatus, this);
		
		this.graphMargin = settings.graphMargin ? settings.graphMargin : 200;
		this.svgns = "http://www.w3.org/2000/svg";
		this.xlinksn = "http://www.w3.org/1999/xlink";
		
		var graphDiv = this.el[0];
		var svg = $(this.el).find('svg')[0];
		this.svgGraph = svg;
		
		var gNode = document.createElementNS(this.svgns, 'g');
		svg.appendChild(gNode);
		this.mainG = gNode;
		if (settings.rightClick) {
			this.rightClick = settings.rightClick;
		}

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

		// Clean everything
		while (this.mainG.lastChild) {
			this.mainG.removeChild(this.mainG.lastChild);
		}

		var data = this.model.get("data");
		var nodes = data.nodes;
		var edges = data.edges;
		if (nodes.length == 0) {
			console.log("No results");
			return;
		};
	
		nodes.sort();
		edges.sort();
		// layout
		layoutGraph(nodes, edges);
		
		var bounds = {};
		this.nodes = {};
		for (var i = 0; i < nodes.length; ++i) {
			this.nodes[nodes[i].id] = nodes[i];
		}
		
		for (var i = 0; i < edges.length; ++i) {
			var inNodes = this.nodes[edges[i].target].inNodes;
			if (!inNodes) {
				inNodes = {};
				this.nodes[edges[i].target].inNodes = inNodes;
			}
			inNodes[edges[i].from] = this.nodes[edges[i].from];
			
			var outNodes = this.nodes[edges[i].from].outNodes;
			if (!outNodes) {
				outNodes = {};
				this.nodes[edges[i].from].outNodes = outNodes;
			}
			outNodes[edges[i].target] = this.nodes[edges[i].target];

			this.drawEdge(this, edges[i]);
		}
		
		this.gNodes = {};
		for (var i = 0; i < nodes.length; ++i) {
			this.drawNode(this, nodes[i], bounds);
		}
		
		this.model.set({"nodes": this.nodes, "edges": edges});
		
		var margin = this.graphMargin;
		bounds.minX = bounds.minX ? bounds.minX - margin : -margin;
		bounds.minY = bounds.minY ? bounds.minY - margin : -margin;
		bounds.maxX = bounds.maxX ? bounds.maxX + margin : margin;
		bounds.maxY = bounds.maxY ? bounds.maxY + margin : margin;
		
		this.assignInitialStatus(self);
		this.handleDisabledChange(self);
		this.graphBounds = bounds;
		this.resetPanZoom(0);
	},
	handleDisabledChange: function(evt) {
		var disabledMap = this.model.get("disabled");

		for(var id in this.nodes) {
			 var g = this.gNodes[id];
			if (disabledMap[id]) {
				this.nodes[id].disabled = true;
				addClass(g, "disabled");
			}
		    else {
		    	this.nodes[id].disabled = false;
		    	removeClass(g, "disabled");
			}
		}
	},
	assignInitialStatus: function(evt) {
		var data = this.model.get("data");
		for (var i = 0; i < data.nodes.length; ++i) {
			var updateNode = data.nodes[i];
			var g = this.gNodes[updateNode.id];
			addClass(g, updateNode.status);
		}
	},
	changeSelected: function(self) {
		console.log("change selected");
		var selected = this.model.get("selected");
		var previous = this.model.previous("selected");
		
		if (previous) {
			// Unset previous
			var g = this.gNodes[previous];
			removeClass(g, "selected");
		}
		
		if (selected) {
			var g = this.gNodes[selected];
			var node = this.nodes[selected];
			
			addClass(g, "selected");
			
			var offset = 200;
			var widthHeight = offset*2;
			var x = node.x - offset;
			var y = node.y - offset;
			
			$(this.svgGraph).svgNavigate("transformToBox", {x: x, y: y, width: widthHeight, height: widthHeight});
		}
	},
	handleStatusUpdate: function(evt) {
		var updateData = this.model.get("update");
		if (updateData.nodes) {
			for (var i = 0; i < updateData.nodes.length; ++i) {
				var updateNode = updateData.nodes[i];
				
				var g = this.gNodes[updateNode.id];
				this.handleRemoveAllStatus(g);
				
				addClass(g, updateNode.status);
			}
		}
	},
	handleRemoveAllStatus: function(gNode) {
		for (var j = 0; j < statusList.length; ++j) {
			var status = statusList[j];
			removeClass(gNode, status);
		}
	},
	clickGraph: function(self) {
		console.log("click");
		if (self.currentTarget.jobid) {
			this.model.set({"selected": self.currentTarget.jobid});
		}
	},
	handleRightClick: function(self) {
		if (this.rightClick) {
			var callbacks = this.rightClick;
			var currentTarget = self.currentTarget;
			if (callbacks.node && currentTarget.jobid) {
				callbacks.node(self);
			}
			else if (callbacks.edge && (currentTarget.nodeName == "polyline" || currentTarget.nodeName == "line")) {
				callbacks.edge(self);
			}
			else if (callbacks.graph) {
				callbacks.graph(self);
			}
			return false;
		}
	
		return true;
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
	drawNode: function(self, node, bounds) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var xOffset = 10;
		var yOffset = 10;
		
		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttributeNS(null, "class", "jobnode");
		nodeG.setAttributeNS(null, "font-family", "helvetica");
		nodeG.setAttributeNS(null, "transform", "translate(" + node.x + "," + node.y + ")");
		this.gNodes[node.id] = nodeG;
		
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
	},
	addBounds: function(toBounds, addBounds) {
		toBounds.minX = toBounds.minX ? Math.min(toBounds.minX, addBounds.minX) : addBounds.minX;
		toBounds.minY = toBounds.minY ? Math.min(toBounds.minY, addBounds.minY) : addBounds.minY;
		toBounds.maxX = toBounds.maxX ? Math.max(toBounds.maxX, addBounds.maxX) : addBounds.maxX;
		toBounds.maxY = toBounds.maxY ? Math.max(toBounds.maxY, addBounds.maxY) : addBounds.maxY;
	},
	resetPanZoom : function(duration) {
		var bounds = this.graphBounds;
		var param = {x: bounds.minX, y: bounds.minY, width: (bounds.maxX - bounds.minX), height: (bounds.maxY - bounds.minY), duration: duration };

		$(this.svgGraph).svgNavigate("transformToBox", param);
	}
});
