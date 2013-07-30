azkaban.FlowExtendedViewPanel = Backbone.View.extend({
	events: {
		"click .closeInfoPanel" : "handleClosePanel" 
	},
	initialize: function(settings) {
		//this.model.bind('change:flowinfo', this.changeFlowInfo, this);
		$(this.el).show();
		$(this.el).draggable({cancel: ".dataContent", containment: "document"});
		
		this.extendedViewPanels = {};
		this.extendedDataModels = {};
		this.render();
		$(this.el).hide();
	},
	showExtendedView: function(evt) {
		var event = evt;
		
		$(this.el).css({top: evt.pageY, left: evt.pageX});
		$(this.el).show();
	},
	render: function(self) {
		console.log("Changing title");
		$(this.el).find(".nodeId").text(this.model.get("id"));
		$(this.el).find(".nodeType").text(this.model.get("type"));
		
		var props = this.model.get("props");
		var tableBody = $(this.el).find(".dataPropertiesBody");
		
		for (var key in props) {
			var tr = document.createElement("tr");
			var tdKey = document.createElement("td");
			var tdValue = document.createElement("td");
			
			$(tdKey).text(key);
			$(tdValue).text(props[key]);
			
			$(tr).append(tdKey);
			$(tr).append(tdValue);
			
			$(tableBody).append(tr);
			
			var propsTable = $(this.el).find(".dataJobProperties");
			$(propsTable).resizable({handler: "s"});
		}
		
		if (this.model.get("type") == "flow") {
			var svgns = "http://www.w3.org/2000/svg";
			var svgDataFlow = $(this.el).find(".dataFlow");
			
			var svgGraph = document.createElementNS(svgns, "svg");
			$(svgGraph).attr("class", "svgTiny");
			$(svgDataFlow).append(svgGraph);
			$(svgDataFlow).resizable();
			
			this.innerGraphModel = new azkaban.GraphModel();
			this.innerGraphModel.set({"data": this.model.get("flow")});
			
			this.graphView = new azkaban.SvgGraphView({el: svgDataFlow, model: this.innerGraphModel, render: true, rightClick:  { "node": nodeClickCallback, "graph": graphClickCallback }})
		}
		else {
			$(this.el).find(".dataFlow").hide();
		}
	},
	handleClosePanel: function(self) {
		$(this.el).hide();
	}
});