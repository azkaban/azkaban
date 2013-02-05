function ajaxCall(requestURL, data, callback) {
	$.get(
		requestURL,
		data,
		function(data) {
			if (data.error == "session") {
				// We need to relogin.
				var errorDialog = document.getElementById("invalid-session");
				if (errorDialog) {
					  $(errorDialog).modal({
					      closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
					      position: ["20%",],
					      containerId: 'confirm-container',
					      containerCss: {
					        'height': '220px',
					        'width': '565px'
					      },
					      onClose: function (dialog) {
					      	window.location.reload();
					      }
					    });
				}
			}
			else {
				callback.call(this,data);
			}
		},
		"json"
	);
}

function executeFlow(executingData) {
	executeURL = contextURL + "/executor";
	$.get(
		executeURL,
		executingData,
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
}

/**
* Checks to see if a flow is running.
*
*/
function flowExecutingStatus(projectId, flowId) {
	var requestURL = contextURL + "/executor";
	
	var executionIds;
	$.ajax( {
		url: requestURL,
		async: false,
		data: {"ajax":"getRunning", "project":projectId, "flow":flowId},
		error: function(data) {},
		success: function(data) {
			if (data.error == "session") {
				// We need to relogin.
				var errorDialog = document.getElementById("invalid-session");
				if (errorDialog) {
					  $(errorDialog).modal({
					      closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
					      position: ["20%",],
					      containerId: 'confirm-container',
					      containerCss: {
					        'height': '220px',
					        'width': '565px'
					      },
					      onClose: function (dialog) {
					      	window.location.reload();
					      }
					    });
				}
			}
			else {
				executionIds = data.execIds;
			}
		}
	});
	
	return executionIds;
}
