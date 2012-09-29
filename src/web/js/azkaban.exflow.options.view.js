var executeFlowView;
azkaban.ExecuteFlowView = Backbone.View.extend({
  	  events : {
  	  	"click" : "closeEditingTarget",
	    "click #execute-btn": "handleExecuteFlow",
	    "click #execute-custom-btn": "handleCustomFlow",
	    "click #cancel-btn": "handleCancelExecution",
	    "click .modal-close": "handleCancelExecution",
	    "click #generalOptions": "handleGeneralOptionsSelect",
	    "click #flowOptions": "handleFlowOptionsSelect",
	    "click #addRow": "handleAddRow",
	    "click table .editable": "handleEditColumn",
	    "click table .removeIcon": "handleRemoveColumn"
	  },
	  initialize: function(evt) {
	  	 $('#executebtn').click( function() {
	  	 	$('#modalBackground').show();
	  	 	$('#executing-options').show();
	  	 });
	  	
	  	 this.handleGeneralOptionsSelect();
	  },
	  handleCancelExecution: function(evt) {
	  	var executeURL = contextURL + "/executor";
		$('#modalBackground').hide();
	  	$('#executing-options').hide();
	  },
	  handleGeneralOptionsSelect: function(evt) {
	  	$('#flowOptions').removeClass('selected');
	  	$('#generalOptions').addClass('selected');

	  	$('#generalPanel').show();	  	
	  	$('#graphPanel').hide();
	  },
	  handleFlowOptionsSelect: function(evt) {
	  	$('#generalOptions').removeClass('selected');
	  	$('#flowOptions').addClass('selected');

	  	$('#graphPanel').show();	  	
	  	$('#generalPanel').hide();
	  },
	  handleExecuteFlow: function(evt) {
	  	var executeURL = contextURL + "/executor";
		$.get(
			executeURL,
			{"project": projectName, "ajax":"executeFlow", "flow":flowName, "disabled":graphModel.get("disabled")},
			function(data) {
				if (data.error) {
					alert(data.error);
				}
				else {
					var redirectURL = contextURL + "/executor?execid=" + data.execid;
					window.location.href = redirectURL;
				}
			},
			"json"
		);
	  },
	  handleCustomFlow: function(evt) {
	  	
	  },
	  handleAddRow: function(evt) {
	  	var tr = document.createElement("tr");
	  	var tdName = document.createElement("td");
	    var tdValue = document.createElement("td");
	    
	    var icon = document.createElement("span");
	    $(icon).addClass("removeIcon");
	    var nameData = document.createElement("span");
	    $(nameData).addClass("spanValue");
	    var valueData = document.createElement("span");
	    $(valueData).addClass("spanValue");
	    	    
		$(tdName).append(icon);
		$(tdName).append(nameData);
		$(tdName).addClass("name");
		$(tdName).addClass("editable");
		
		$(tdValue).append(valueData);
	    $(tdValue).addClass("editable");
		
	  	$(tr).append(tdName);
	  	$(tr).append(tdValue);
	   
	  	$(tr).insertBefore("#addRow");
	  },
	  handleEditColumn : function(evt) {
	  	var curTarget = evt.currentTarget;
	
	  	if (this.editingTarget != curTarget) {
			this.closeEditingTarget();
			
			var text = $(curTarget).children(".spanValue").text();
			$(curTarget).empty();
						
			var input = document.createElement("input");
			$(input).attr("type", "text");
			$(input).css("width", "100%");
			$(input).val(text);
			$(curTarget).addClass("editing");
			$(curTarget).append(input);
			$(input).focus();
			this.editingTarget = curTarget;
	  	}
	  },
	  handleRemoveColumn : function(evt) {
	  	var curTarget = evt.currentTarget;
	  	// Should be the table
	  	var row = curTarget.parentElement.parentElement;
		$(row).remove();
	  },
	  handleResetData : function(evt) {
	  },
	  closeEditingTarget: function(evt) {
	  	if (this.editingTarget != null && this.editingTarget != evt.target && this.editingTarget != evt.target.parentElement ) {
	  		var input = $(this.editingTarget).children("input")[0];
	  		var text = $(input).val();
	  		$(input).remove();

		    var valueData = document.createElement("span");
		    $(valueData).addClass("spanValue");
		    $(valueData).text(text);

	  		if ($(this.editingTarget).hasClass("name")) {
		  		var icon = document.createElement("span");
		    	$(icon).addClass("removeIcon");
		    	$(this.editingTarget).append(icon);
		    }
		    
		    $(this.editingTarget).removeClass("editing");
		    $(this.editingTarget).append(valueData);
		    this.editingTarget = null;
	  	}
	  },
	  addRowData : function(evt) {

	  }
});