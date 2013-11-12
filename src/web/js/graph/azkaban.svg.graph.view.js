function hasClass(el, name) {
	var classes = el.getAttribute("class");
	if (classes == null) {
		return false;
	}
	return new RegExp('(\\s|^)'+name+'(\\s|$)').test(classes);
}

function addClass(el, name) {
	if (!hasClass(el, name)) { 
		var classes = el.getAttribute("class");
		classes += classes ? ' ' + name : '' +name;
		el.setAttribute("class", classes);
	}
}

function removeClass(el, name) {
	if (hasClass(el, name)) {
		var classes = el.getAttribute("class");
		el.setAttribute("class", classes.replace(
				new RegExp('(\\s|^)'+name+'(\\s|$)'),' ').replace(/^\s+|\s+$/g, ''));
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
		this.model.bind('centerNode', this.centerNode, this);
		this.model.bind('change:graph', this.render, this);
		this.model.bind('resetPanZoom', this.resetPanZoom, this);
		this.model.bind('change:update', this.handleStatusUpdate, this);
		this.model.bind('change:disabled', this.handleDisabledChange, this);
		this.model.bind('change:updateAll', this.handleUpdateAllStatus, this);
		
		this.graphMargin = settings.graphMargin ? settings.graphMargin : 25;
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
		
		if (settings.render) {
			this.render();
		}
	},
	
	initializeDefs: function(self) {
		var def = document.createElementNS(svgns, 'defs');
		def.setAttribute("id", "buttonDefs");

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
		
		var bounds = {};
		this.nodes = {};
		for (var i = 0; i < nodes.length; ++i) {
			this.nodes[nodes[i].id] = nodes[i];
			nodes[i].label = nodes[i].id;
		}
		
		this.gNodes = {};
		for (var i = 0; i < nodes.length; ++i) {
			this.drawNode(this, nodes[i]);
		}
		
		// layout
		layoutGraph(nodes, edges, 10);
		this.moveNodes(bounds);
		
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
		
		this.model.set({
			"flowId": data.flowId, 
			"nodes": this.nodes, 
			"edges": edges
		});
		
		var margin = this.graphMargin;
		bounds.minX = bounds.minX ? bounds.minX - margin : -margin;
		bounds.minY = bounds.minY ? bounds.minY - margin : -margin;
		bounds.maxX = bounds.maxX ? bounds.maxX + margin : margin;
		bounds.maxY = bounds.maxY ? bounds.maxY + margin : margin;
		
		this.assignInitialStatus(self);
		
		if (this.model.get("disabled")) {
			this.handleDisabledChange(self);
		}
		else {
			this.model.set({"disabled":[]})
		}
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
			if (updateNode.status) {
				addClass(g, updateNode.status);
			}
			else {
				addClass(g, "READY");
			}
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
			
			console.log(this.model.get("autoPanZoom"));
			if (this.model.get("autoPanZoom")) {
				var offset = 150;
				var widthHeight = offset*2;
				var x = node.x - offset;
				var y = node.y - offset;
				
				$(this.svgGraph).svgNavigate("transformToBox", {
					x: x, 
					y: y, 
					width: widthHeight, 
					height: widthHeight
				});
			}
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
				callbacks.node(self, this.model);
			}
			else if (callbacks.edge && 
					(currentTarget.nodeName == "polyline" || 
					 currentTarget.nodeName == "line")) {
				callbacks.edge(self, this.model);
			}
			else if (callbacks.graph) {
				callbacks.graph(self, this.model);
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
		
		var startPointY = startNode.y + startNode.height/2 - 3;
		var endPointY = endNode.y - endNode.height/2 + 3;
		if (edge.guides) {
			var pointString = "" + startNode.x + "," + startPointY + " ";

			for (var i = 0; i < edge.guides.length; ++i ) {
				edgeGuidePoint = edge.guides[i];
				pointString += edgeGuidePoint.x + "," + edgeGuidePoint.y + " ";
			}
			
			pointString += endNode.x + "," + endPointY;
			var polyLine = document.createElementNS(svgns, "polyline");
			polyLine.setAttribute("class", "edge");
			polyLine.setAttribute("points", pointString);
			polyLine.setAttribute("style", "fill:none;");
			$(self.mainG).prepend(polyLine);
		}
		else { 
			var line = document.createElementNS(svgns, 'line');
			line.setAttribute("class", "edge");
			line.setAttribute("x1", startNode.x);
			line.setAttribute("y1", startPointY);
			line.setAttribute("x2", endNode.x);
			line.setAttribute("y2", endPointY);
			
			$(self.mainG).prepend(line);
		}
	},
	drawNode: function(self, node) {
		if (node.type == 'flow') {
			this.drawFlowNode(self, node);
		}
		else {
			this.drawBoxNode(self,node);
			//this.drawCircleNode(self,node,bounds);
		}
	},
	moveNodes: function(bounds) {
		var nodes = this.nodes;
		var gNodes = this.gNodes;
		
		for (var key in nodes) {
			var node = nodes[key];
			var gNode = gNodes[node.id];
			var centerX = node.centerX;
			var centerY = node.centerY;
			
			gNode.setAttribute("transform", "translate(" + node.x + "," + node.y + ")");
			this.addBounds(bounds, {
				minX: node.x - centerX, 
				minY: node.y - centerY, 
				maxX: node.x + centerX, 
				maxY: node.y + centerY
			});
		}
	},
	drawFlowNode: function(self, node) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var height = 26;
		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttribute("class", "jobnode");
		nodeG.setAttribute("font-family", "helvetica");
		nodeG.setAttribute("transform", "translate(" + node.x + "," + node.y + ")");
		this.gNodes[node.id] = nodeG;
		
		var innerG = document.createElementNS(svgns, "g");
		innerG.setAttribute("class", "nodebox");
		
		var rect = document.createElementNS(svgns, 'rect');
		rect.setAttribute("rx", 3);
		rect.setAttribute("ry", 5);
		rect.setAttribute("style", "width:inherit;stroke-opacity:1");
		//rect.setAttribute("class", "nodecontainer");
		
		var text = document.createElementNS(svgns, 'text');
		var textLabel = document.createTextNode(node.label);
		text.appendChild(textLabel);
		text.setAttribute("y", 1);
		text.setAttribute("height", 10);

		var flowIdText = document.createElementNS(svgns, 'text');
		var flowIdLabel = document.createTextNode(node.flowId);
		flowIdText.appendChild(flowIdLabel);
		flowIdText.setAttribute("y", 11);
		flowIdText.setAttribute("font-size", 8);
		
		var iconHeight = 20;
		var iconWidth = 21;
		var iconMargin = 4;
		var iconNode = document.createElementNS(svgns, 'image');
		iconNode.setAttribute("width", iconHeight);
		iconNode.setAttribute("height", iconWidth);
		iconNode.setAttribute("x", 0);
		iconNode.setAttribute("y", -10);
		iconNode.setAttributeNS('http://www.w3.org/1999/xlink', "xlink:href", contextURL + "/images/graph-icon.png");
		
		innerG.appendChild(rect);
		innerG.appendChild(text);
		innerG.appendChild(flowIdText);
		innerG.appendChild(iconNode);
		innerG.jobid = node.id;
		innerG.jobtype = "flow";
		innerG.flowId = node.flowId;

		nodeG.appendChild(innerG);
		self.mainG.appendChild(nodeG);

		var horizontalMargin = 8;
		var verticalMargin = 2;
		
		// Need to get text width after attaching to SVG.
		var subLabelTextLength = flowIdText.getComputedTextLength();
		
		var computeTextLength = text.getComputedTextLength();
		var computeTextHeight = 22;
		
		var width = computeTextLength > subLabelTextLength ? computeTextLength : subLabelTextLength;
		width += iconWidth + iconMargin;
		var halfWidth = width/2;
		var halfHeight = height/2;
		
		// Margin for surrounding box.
		var boxWidth = width + horizontalMargin * 2;
		var boxHeight = height + verticalMargin * 2;
		
		node.width = boxWidth;
		node.height = boxHeight;
		node.centerX = boxWidth/2;
		node.centerY = boxHeight/2;
		
		var textXOffset = -halfWidth + iconWidth + iconMargin;
		iconNode.setAttribute("x", -halfWidth);
		text.setAttribute("x", textXOffset);
		flowIdText.setAttribute("x",textXOffset);
		rect.setAttribute("x", -node.centerX);
		rect.setAttribute("y", -node.centerY);
		rect.setAttribute("width", node.width);
		rect.setAttribute("height", node.height);
		
		nodeG.setAttribute("class", "node");
		nodeG.jobid=node.id;
	},
	drawBoxNode: function(self, node) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var height = 18;
		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttribute("class", "jobnode");
		nodeG.setAttribute("font-family", "helvetica");
		nodeG.setAttribute("transform", "translate(" + node.x + "," + node.y + ")");
		this.gNodes[node.id] = nodeG;
		
		var innerG = document.createElementNS(svgns, "g");
		innerG.setAttribute("class", "nodebox");
		
		var rect = document.createElementNS(svgns, 'rect');
		rect.setAttribute("rx", 3);
		rect.setAttribute("ry", 5);
		rect.setAttribute("style", "width:inherit;stroke-opacity:1");
		//rect.setAttribute("class", "nodecontainer");
		
		var text = document.createElementNS(svgns, 'text');
		var textLabel = document.createTextNode(node.label);
		text.appendChild(textLabel);
		text.setAttribute("y", 6);
		text.setAttribute("height", 10); 

		/*this.addBounds(bounds, {
			minX: node.x - xOffset, 
			minY: node.y - yOffset, 
			maxX: node.x + xOffset, 
			maxY: node.y + yOffset
		});*/

		innerG.appendChild(rect);
		innerG.appendChild(text);
		innerG.jobid = node.id;

		nodeG.appendChild(innerG);
		self.mainG.appendChild(nodeG);

		var horizontalMargin = 8;
		var verticalMargin = 2;
		
		// Need to get text width after attaching to SVG.
		var computeText = text.getComputedTextLength();
		var computeTextHeight = 22;
		var halfWidth = computeText/2;
		var halfHeight = height/2;
		
		// Margin for surrounding box.
		var boxWidth = computeText + horizontalMargin * 2;
		var boxHeight = height + verticalMargin * 2;
		
		node.width = boxWidth;
		node.height = boxHeight;
		node.centerX = boxWidth/2;
		node.centerY = boxHeight/2;
		
		text.setAttribute("x", -halfWidth);
		rect.setAttribute("x", -node.centerX);
		rect.setAttribute("y", -node.centerY);
		rect.setAttribute("width", node.width);
		rect.setAttribute("height", node.height);
		
		nodeG.setAttribute("class", "node");
		nodeG.jobid=node.id;
	},
	drawCircleNode: function(self, node, bounds) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var xOffset = 10;
		var yOffset = 10;
		
		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttribute("class", "jobnode");
		nodeG.setAttribute("font-family", "helvetica");
		nodeG.setAttribute("transform", "translate(" + node.x + "," + node.y + ")");
		this.gNodes[node.id] = nodeG;
		
		var innerG = document.createElementNS(svgns, "g");
		innerG.setAttribute("transform", "translate(-10,-10)");
		
		var circle = document.createElementNS(svgns, 'circle');
		circle.setAttribute("cy", 10);
		circle.setAttribute("cx", 10);
		circle.setAttribute("r", 12);
		circle.setAttribute("style", "width:inherit;stroke-opacity:1");
		//circle.setAttribute("class", "border");
		//circle.setAttribute("class", "nodecontainer");
		
		var text = document.createElementNS(svgns, 'text');
		var textLabel = document.createTextNode(node.label);
		text.appendChild(textLabel);
		text.setAttribute("x", 0);
		text.setAttribute("y", 0);
				
		this.addBounds(bounds, {
			minX: node.x - xOffset, 
			minY: node.y - yOffset, 
			maxX: node.x + xOffset, 
			maxY: node.y + yOffset
		});
		
		var backRect = document.createElementNS(svgns, 'rect');
		backRect.setAttribute("x", 0);
		backRect.setAttribute("y", 2);
		backRect.setAttribute("class", "backboard");
		backRect.setAttribute("width", 10);
		backRect.setAttribute("height", 15);
		
		innerG.appendChild(circle);
		innerG.appendChild(backRect);
		innerG.appendChild(text);
		innerG.jobid = node.id;

		nodeG.appendChild(innerG);
		self.mainG.appendChild(nodeG);

		// Need to get text width after attaching to SVG.
		var computeText = text.getComputedTextLength();
		var halfWidth = computeText/2;
		text.setAttribute("x", -halfWidth + 10);
		backRect.setAttribute("x", -halfWidth);
		backRect.setAttribute("width", computeText + 20);

		nodeG.setAttribute("class", "node");
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
		var param = {
			x: bounds.minX, 
			y: bounds.minY, 
			width: (bounds.maxX - bounds.minX), 
			height: (bounds.maxY - bounds.minY), 
			duration: duration 
		};

		this.panZoom(param);
	},
	centerNode: function(jobId) {
		var node = this.nodes[jobId];
		
		var offset = 150;
		var widthHeight = offset*2;
		var x = node.x - offset;
		var y = node.y - offset;
		
		this.panZoom({x: x, y: y, width: widthHeight, height: widthHeight});
	},
	panZoom: function(params) {
		params.maxScale = 2;
		$(this.svgGraph).svgNavigate("transformToBox", params);
	}
});
