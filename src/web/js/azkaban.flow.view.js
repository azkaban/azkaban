$.namespace('azkaban');

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

var svgGraphView;
azkaban.SvgGraphView = Backbone.View.extend({
	events: {
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.changeSelected, this);
		this.model.bind('change:graph', this.render, this);
		
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
	render: function(self) {
		console.log("graph render");

		var data = this.model.get("data");
		var nodes = data.nodes;
		for (var i = 0; i < nodes.length; ++i) {
			this.drawNode(this, nodes[i]);
		}
	},
	changeSelected: function(self) {
		console.log("change selected");
	},
	drawNode: function(self, node) {
		var svg = self.svgGraph;
		var svgns = self.svgns;

		var nodeG = document.createElementNS(svgns, "g");
		nodeG.setAttributeNS(null, "id", node.id);
		nodeG.setAttributeNS(null, "font-family", "helvetica");
		nodeG.setAttributeNS(null, "transform", "translate(" + (node.x * 100) + "," + (node.y*100)+ ")");
		
		var rect1 = document.createElementNS(svgns, 'rect');
		rect1.setAttributeNS(null, "y", 2);
		rect1.setAttributeNS(null, "x", 2);
		rect1.setAttributeNS(null, "ry", 12);
		rect1.setAttributeNS(null, "width", 20);
		rect1.setAttributeNS(null, "height", 30);
		rect1.setAttributeNS(null, "style", "width:inherit;fill-opacity:1.0;stroke-opacity:1");
		
		nodeG.appendChild(rect1);
		self.mainG.appendChild(nodeG);
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
	
	var requestURL = contextURL + "/manager";

	$.get(
	      requestURL,
	      {"project": projectName, "json":"fetchflowgraph", "flow":flowName},
	      function(data) {
	          console.log("data fetched");
	          graphModel.set({data: data});
	          graphModel.trigger("change:graph");
	      },
	      "json"
	    );
});
