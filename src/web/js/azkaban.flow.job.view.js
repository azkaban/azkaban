azkaban.JobListView = Backbone.View.extend({
	events: {
		"keyup input": "filterJobs",
		"click li": "handleJobClick",
		"click .resetPanZoomBtn" : "handleResetPanZoom"
	},
	initialize: function(settings) {
		this.model.bind('change:selected', this.handleSelectionChange, this);
		this.model.bind('change:disabled', this.handleDisabledChange, this);
		this.model.bind('change:graph', this.render, this);
		this.model.bind('change:update', this.handleStatusUpdate, this);
		
		this.filterInput = $(this.el).find(".filter");
		this.list = $(this.el).find(".list");
		this.contextMenu = settings.rightClick;
		this.listNodes = {};
	},
	filterJobs: function(self) {
		var filter = this.filterInput.val();
		
		if (filter && filter.trim() != "") {
			filter = filter.trim();
			
			if (filter == "") {
				if (this.filter) {
					this.jobs.children().each(
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
				this.jobs.children().each(
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
		
		this.jobs.children().each(
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
	handleStatusUpdate: function(evt) {
		var updateData = this.model.get("update");
		for (var i = 0; i < updateData.nodes.length; ++i) {
			var updateNode = updateData.nodes[i];
			$(this.listNodes[updateNode.id]).addClass(updateNode.status);
		}
	},
	assignInitialStatus: function(evt) {
		var data = this.model.get("data");
		for (var i = 0; i < data.nodes.length; ++i) {
			var updateNode = data.nodes[i];
			
			$(this.listNodes[updateNode.id]).addClass(updateNode.status);
		}
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
		$(ul).attr("class", "jobs");
		this.jobs = $(ul);
		
		for (var i = 0; i < nodeArray.length; ++i) {
			var li = document.createElement("li");
			var iconDiv = document.createElement("div");
			$(iconDiv).addClass("icon");
			li.appendChild(iconDiv);
			
			var a = document.createElement("a");
			$(a).text(nodeArray[i].id);
			li.appendChild(a);
			ul.appendChild(li);
			li.jobid=nodeArray[i].id;
			
			$(li).contextMenu({
					menu: this.contextMenu.id
				},
				this.contextMenu.callback
			);
			
			this.listNodes[nodeArray[i].id] = li;
		}
		
		this.list.append(ul);
		this.assignInitialStatus(self);
		this.handleDisabledChange(self);
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
		var nodes = this.model.get("nodes");
		
		for(var id in nodes) {
			if (disabledMap[id]) {
				$(this.listNodes[id]).addClass("nodedisabled");
			}
			else {
				$(this.listNodes[id]).removeClass("nodedisabled");
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
