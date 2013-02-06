$.namespace('azkaban');


function removeSched(projectId, flowName) {
	var scheduleURL = contextURL + "/schedule"
	var redirectURL = contextURL + "/schedule"
	$.post(
			scheduleURL,
			{"action":"removeSched", "projectId":projectId, "flowName":flowName},
			function(data) {
				if (data.error) {
//                 alert(data.error)
					$('#errorMsg').text(data.error)
				}
				else {
// 		 alert("Schedule "+schedId+" removed!")
					window.location = redirectURL
				}
			},
			"json"
	)
}

function removeSla(projectId, flowName) {
	var scheduleURL = contextURL + "/schedule"
	var redirectURL = contextURL + "/schedule"
	$.post(
			scheduleURL,
			{"action":"removeSla", "projectId":projectId, "flowName":flowName},
			function(data) {
				if (data.error) {
//                 alert(data.error)
					$('#errorMsg').text(data.error)
				}
				else {
// 		 alert("Schedule "+schedId+" removed!")
					window.location = redirectURL
				}
			},
			"json"
	)
}

azkaban.ChangeSlaView = Backbone.View.extend({
	events : {
		"click" : "closeEditingTarget",
		"click #set-sla-btn": "handleSetSla",	
		"click #remove-sla-btn": "handleRemoveSla",
		"click #sla-cancel-btn": "handleSlaCancel",
		"click .modal-close": "handleSlaCancel",
	},
	initialize: function(setting) {

	},
	handleSlaCancel: function(evt) {
		console.log("Clicked cancel button");
		var scheduleURL = contextURL + "/schedule";

		$('#slaModalBackground').hide();
		$('#sla-options').hide();
	},
	initFromSched: function(projId, flowName) {
		this.projectId = projId;
		this.flowName = flowName;
		this.scheduleURL = contextURL + "/schedule"
		var fetchScheduleData = {"projId": this.projectId, "ajax":"schedInfo", "flowName":this.flowName};
		
		$.get(
				this.scheduleURL,
				fetchScheduleData,
				function(data) {
					if (data.error) {
						alert(data.error);
					}
					else {
						if (data.slaEmails) {
							$('#slaEmails').val(data.slaEmails.join());
						}
						var flowRulesTbl = document.getElementById("flowRulesTbl").tBodies[0];
						var flowRuleRow = flowRulesTbl.insertRow(-1);
						var cflowName = flowRuleRow.insertCell(0);
						cflowName.innerHTML = flowName;
						var cflowduration = flowRuleRow.insertCell(1);
						var flowDuration = document.createElement("input");
						flowDuration.setAttribute("type", "text");
						flowDuration.setAttribute("id", "flowDuration");
						flowDuration.setAttribute("class", "durationpick");
						if(data.flowRules) {
							flowDuration.setAttribute("value", data.flowRules.duration);
						}
						cflowduration.appendChild(flowDuration);
						var emailAct = flowRuleRow.insertCell(2);
						var checkEmailAct = document.createElement("input");
						checkEmailAct.setAttribute("type", "checkbox");
						emailAct.appendChild(checkEmailAct);
						var killAct = flowRuleRow.insertCell(3);
						var checkKillAct = document.createElement("input");
						checkKillAct.setAttribute("type", "checkbox");
						killAct.appendChild(checkKillAct);
						
						var jobRulesTbl = document.getElementById("jobRulesTbl").tBodies[0];
						var allJobs = data.allJobs;
						for (var job in allJobs) {
							
							var jobRuleRow = jobRulesTbl.insertRow(-1);
							var cjobName = jobRuleRow.insertCell(0);
							cjobName.innerHTML = allJobs[job];
							var cjobduration = jobRuleRow.insertCell(1);
							var jobDuration = document.createElement("input");
							jobDuration.setAttribute("type", "text");
							jobDuration.setAttribute("id", "jobDuration");
							jobDuration.setAttribute("class", "durationpick");
							if(data.jobRules) {
								jobDuration.setAttribute("value", data.jobRules[job].duration);
							}
							cjobduration.appendChild(jobDuration);
							
							var emailAct = jobRuleRow.insertCell(2);
							var checkEmailAct = document.createElement("input");
							checkEmailAct.setAttribute("type", "checkbox");
							emailAct.appendChild(checkEmailAct);
							var killAct = jobRuleRow.insertCell(3);
							var checkKillAct = document.createElement("input");
							checkKillAct.setAttribute("type", "checkbox");
							killAct.appendChild(checkKillAct);
						}
						$('.durationpick').timepicker({hourMax: 99});
					}
				},
				"json"
			);
		
		$('#slaModalBackground').show();
		$('#sla-options').show();
		
//		this.schedFlowOptions = sched.flowOptions
		console.log("Loaded schedule info. Ready to set SLA.");

	},
	handleRemoveSla: function(evt) {
		console.log("Clicked remove sla button");
		var scheduleURL = contextURL + "/schedule"
		var redirectURL = contextURL + "/schedule"
		$.post(
				scheduleURL,
				{"action":"removeSla", "projectId":this.projectId, "flowName":this.flowName},
				function(data) {
				if (data.error) {
						$('#errorMsg').text(data.error)
					}
					else {
						window.location = redirectURL
					}
				"json"
				}
			);

	},
	handleSetSla: function(evt) {

		var slaEmails = $('#slaEmails').val();

//		var flowRules = {};
		var flowRulesTbl = document.getElementById("flowRulesTbl").tBodies[0];
		var flowRuleRow = flowRulesTbl.rows[0];
//		flowRules["flowDuration"] = flowRuleRow.cells[1].firstChild.value;
//		flowRules["flowEmailAction"] = flowRuleRow.cells[2].firstChild.value;
//		flowRules["flowKillAction"] = flowRuleRow.cells[3].firstChild.value;
		var flowRules = flowRuleRow.cells[1].firstChild.value + ',' + flowRuleRow.cells[2].firstChild.value + ',' + flowRuleRow.cells[3].firstChild.value;
		
		var jobRules = {};
		var jobRulesTbl = document.getElementById("jobRulesTbl").tBodies[0];
		console.log(jobRulesTbl.rows.length);
		for(var row = 0; row < jobRulesTbl.rows.length; row++) {
			
			var jobRow = jobRulesTbl.rows[row];
			var jobRule = {};
			
			console.log(row);
			console.log(jobRow.cells[0].firstChild.value);
//			jobRule["jobDuration"] = jobRow.cells[1].firstChild.value;
//			jobRule["jobEmailAction"] = jobRow.cells[2].firstChild.value;
//			jobRule["jobKillAction"] = jobRow.cells[3].firstChild.value;
//			jobRules[jobRow.cells[0].innerHTML] = jobRule;
			jobRules[jobRow.cells[0].innerHTML] = jobRow.cells[1].firstChild.value + ',' + jobRow.cells[2].firstChild.value + ',' +  jobRow.cells[3].firstChild.value;
		}
		
		var slaData = {
			projectId: this.projectId,
			flowName: this.flowName,
			ajax: "setSla",			
			slaEmails: slaEmails,
			flowRules: flowRules,
			jobRules: jobRules
		};

		$.get(
			this.scheduleURL,
			slaData,
			function(data) {
				if (data.error) {
					alert(data.error);
				}
				else {
					window.location.href = this.scheduleURL;
				}
			},
			"json"
		);
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
	}
});

var slaView;

$(function() {
	var selected;


	slaView = new azkaban.ChangeSlaView({el:$('#sla-options')});
	
//	var requestURL = contextURL + "/manager";

	// Set up the Flow options view. Create a new one every time :p
//	 $('#addSlaBtn').click( function() {
//		 slaView.show();
//	 });

	 
	
});