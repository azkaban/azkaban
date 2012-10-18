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

function ajaxLogsCall(requestURL, data, callback) {
	$.get(
		requestURL,
		data,
		function(data) {
			var pos = data.lastIndexOf("\n");
			var log = data.substring(0, pos);
			var currentPos = data.substr(pos + 1);
			
			var newData = {current: currentPos, log: log};
			
			callback.call(this,newData);
		}
	);
}