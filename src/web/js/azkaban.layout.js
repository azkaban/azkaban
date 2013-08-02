var maxTextSize = 32;
var reductionSize = 26;
var degreeRatio = 1/8;
var maxHeight = 200;
var cornerGap = 10;

function layoutGraph(nodes, edges, hmargin) {
	var startLayer = [];
	var numLayer = 0;
	var nodeMap = {};
	
	var maxLayer = 0;
	var layers = {};
	
	if (!hmargin) {
		hmargin = 8;
	}
	
	// Assign to layers
	for (var i = 0; i < nodes.length; ++i) {
		numLayer = Math.max(numLayer, nodes[i].level);

		var width = nodes[i].width ? nodes[i].width : nodes[i].label.length * 11.5 + 4;
		var height = nodes[i].height ? nodes[i].height : 1;
		var node = { id: nodes[i].id, node: nodes[i], level: nodes[i].level, in:[], out:[], width: width + hmargin, x:0, height:height };
		nodeMap[nodes[i].id] = node;

		maxLayer = Math.max(node.level, maxLayer);
		if(!layers[node.level]) {
			layers[node.level] = [];
		}
		
		layers[node.level].push(node);
	}
	
	// Create dummy nodes
	var edgeDummies = {};
	
	for (var i=0; i < edges.length; ++i ) {
		var edge = edges[i];
		var src = edges[i].from;
		var dest = edges[i].target;
		
		var edgeId = src + ">>" + dest;
		
		var srcNode = nodeMap[src];
		var destNode = nodeMap[dest];
		
		var lastNode = srcNode;

		var guides = [];
		
		for (var j = srcNode.level + 1; j < destNode.level; ++j) {
			var dummyNode = {level: j, in: [], x: lastNode.x, out: [], realSrc: srcNode, realDest: destNode, width: 10, height: 10};
			layers[j].push(dummyNode);
			dummyNode.in.push(lastNode);
			lastNode.out.push(dummyNode);
			lastNode = dummyNode;
			
			guides.push(dummyNode);
		}
		
		destNode.in.push(lastNode);
		lastNode.out.push(destNode);
		
		if (edgeDummies.length != 0) {
			edgeDummies[edgeId] = guides;
		}
	}

	spreadLayerSmart(layers[maxLayer]);
	sort(layers[maxLayer]);
	for (var i=maxLayer - 1; i >=0; --i) {
		uncrossWithOut(layers[i]);
		sort(layers[i]);
		
		spreadLayerSmart(layers[i]);
	}

	// The top level can get out of alignment, so we do this kick back
	// manouver before we seriously get started sorting.
	if (maxLayer > 1) {
		uncrossWithIn(layers[1]);
		sort(layers[1]);
		spreadLayerSmart(layers[1]);

		uncrossWithOut(layers[0]);
		sort(layers[0]);
		spreadLayerSmart(layers[0]);
	}

	// Uncross down
	for (var i=1; i <= maxLayer; ++i) {
		uncrossWithIn(layers[i]);
		sort(layers[i]);
		spreadLayerSmart(layers[i]);
	}
	

	// Space it vertically
	spaceVertically(layers, maxLayer);
	
	// Assign points to nodes
	for (var i = 0; i < nodes.length; ++i) {
		var node = nodes[i];
		var layerNode = nodeMap[node.id];
		node.x = layerNode.x;
		node.y = layerNode.y;
	}
	
	// Dummy node for more points.
	for (var i = 0; i < edges.length; ++i) {
		var edge = edges[i];
		var src = edges[i].from;
		var dest = edges[i].target;
		
		var edgeId = src + ">>" + dest;

		if (edgeDummies[edgeId] && edgeDummies[edgeId].length > 0) {
			var prevX = nodeMap[src].x;
			var destX = nodeMap[dest].x; 
			
			var guides = [];
			var dummies = edgeDummies[edgeId];
			for (var j=0; j< dummies.length; ++j) {
				var point = {x: dummies[j].x, y: dummies[j].y};
				guides.push(point);
				
				var nextX = j == dummies.length - 1 ? destX: dummies[j + 1].x; 

				if (point.x != prevX && point.x != nextX) {
					// Add gap
					if ((point.x > prevX) == (point.x > nextX)) {
						guides.push({x: point.x, y:point.y + cornerGap});
					}
				}
				prevX = point.x;
			}
						
			edge.guides = guides;
		}
		else {
			edge.guides = null;
		}
	}
}

function spreadLayerSmart(layer) {
	var ranges = [];
	ranges.push({start: 0, end:0, width: layer[0].width, x: layer[0].x, index: 0});
	var largestRangeIndex = -1;
	
	var totalX = layer[0].x;
	var totalWidth = layer[0].width;
	var count = 1;
	
	for (var i = 1; i < layer.length; ++i ) {
		var prevRange = ranges[ranges.length - 1];
		var delta = layer[i].x - prevRange.x;
				
		if (delta == 0) {
			prevRange.end = i;
			prevRange.width += layer[i].width;
			totalWidth+=layer[i].width;
		}
		else {
			totalWidth+=Math.max(layer[i].width, delta);
			ranges.push({start: i, end: i, width: layer[i].width, x: layer[i].x, index: ranges.length});
		}
		
		totalX += layer[i].x;
		count++;
	}

	// Space the ranges, but place the left and right most last
	var startIndex = 0;
	var endIndex = 0;
	if (ranges.length == 1) {
		startIndex = -1;
		endIndex = 1;
	}
	else if ((ranges.length % 2) == 1) {
		var index = Math.ceil(ranges.length/2);
		startIndex = index - 1;
		endIndex = index + 1;
	}
	else {
		var e = ranges.length/2;
		var s = e - 1;
		
		var crossPointS = ranges[s].x + ranges[s].width/2;
		var crossPointE = ranges[e].x - ranges[e].width/2;
		
		if (crossPointS > crossPointE) {
			var midPoint = (ranges[s].x + ranges[e].x)/2;
			ranges[s].x = midPoint - ranges[s].width/2;
			ranges[e].x = midPoint + ranges[e].width/2;
		}
		
		startIndex = s - 1;
		endIndex = e + 1;
	}
	
	for (var i=startIndex; i >= 0; --i) {
		var range = ranges[i];
		var crossPointS = range.x + range.width/2;
		var crossPointE = ranges[i + 1].x - ranges[i + 1].width/2;
		
		if (crossPointE < crossPointS) {
			range.x -= crossPointS - crossPointE;
		}
	}
	
	for (var i=endIndex; i < ranges.length; ++i) {
		var range = ranges[i];
		var crossPointE = range.x - range.width/2;
		var crossPointS = ranges[i - 1].x + ranges[i - 1].width/2;
		
		if (crossPointE < crossPointS) {
			range.x += crossPointS - crossPointE;
		}
	}
	
	for (var i=0; i < ranges.length; ++i) {
		var range = ranges[i];
		if (range.start == range.end) {
			layer[range.start].x = range.x;
		}
		else {
			var start = range.x - range.width/2;
			
			for (var j=range.start;j <=range.end; ++j) {
				layer[j].x = start + layer[j].width/2;
				start += layer[j].width;
			}
		}
	}
}


function spaceVertically(layers, maxLayer) {
	var startY = 0;
	var startLayer = layers[0];
	var startMaxHeight = 1;
	for (var i=0; i < startLayer.length; ++i) {
		startLayer[i].y = startY;
		startMaxHeight = Math.max(startMaxHeight, startLayer[i].height);
	}
	
	var minHeight = 40;
	for (var a=1; a <= maxLayer; ++a) {
		var maxDelta = 0;
		var layer = layers[a];
		
		var layerMaxHeight = 1;
		for (var i=0; i < layer.length; ++i) {
			layerMaxHeight = Math.max(layerMaxHeight, layer[i].height);

			for (var j=0; j < layer[i].in.length; ++j) {
				var upper = layer[i].in[j];
				var delta = Math.abs(upper.x - layer[i].x);
				
				maxDelta = Math.max(maxDelta, delta);
			}
		}
		
		console.log("Max " + maxDelta);
		var calcHeight = maxDelta*degreeRatio;
		
		var newMinHeight = minHeight + startMaxHeight/2 + layerMaxHeight / 2;
		startMaxHeight = layerMaxHeight;

		startY += Math.max(calcHeight, newMinHeight);
		for (var i=0; i < layer.length; ++i) {
			layer[i].y=startY;
		}
	}

}

function uncrossWithIn(layer) {
	for (var i = 0; i < layer.length; ++i) {
		var pos = findAverage(layer[i].in);
		layer[i].x = pos;
	}
}

function findAverage(nodes) {
	var sum = 0;
	for (var i = 0; i < nodes.length; ++i) {
		sum += nodes[i].x;
	}
	
	return sum/nodes.length;
}

function uncrossWithOut(layer) {
	for (var i = 0; i < layer.length; ++i) {
		var pos = findAverage(layer[i].out);
		layer[i].x = pos;
	}
}

function sort(layer) {
	layer.sort(function(a, b) {
		return a.x - b.x;
	});
}
