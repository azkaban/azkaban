var getDuration = function(startMs, endMs) {
	if (startMs) {
		if (endMs == null) {
			if (data.status == "running") {
				endMs = currentTime;
			}
			else {
				return "-";
			}
		}
		
		var diff = endMs - startMs;
		var seconds = Math.floor(diff / 1000);
		
		if (seconds < 60) {
			return seconds + " sec";
		}
		
		var mins = Math.floor(seconds / 60);
		seconds = seconds % 60;
		if (mins < 60) {
			return mins + "m " + seconds + "s";
		}

		var hours = Math.floor(mins / 60);
		mins = mins % 60;
		if (hours < 24) {
			return hours + "h " + mins + " m" + seconds + "s";
		}
		
		var days = Math.floor(hours / 24);
		hours = hours % 24;
		
		return days + "d " + hours + "h " + mins + "m " + seconds + "s";
	}

	return "-";
}

var getDateFormat = function(date) {
	var year = date.getFullYear();
	var month = getTwoDigitStr(date.getMonth());
	var day = getTwoDigitStr(date.getDate());
	
	var hours = getTwoDigitStr(date.getHours());
	var minutes = getTwoDigitStr(date.getMinutes());
	var second = getTwoDigitStr(date.getSeconds());

	var datestring = year + "-" + month + "-" + day + "  " + hours + ":" + minutes + " " + second + "s";
	return datestring;
}

var getTwoDigitStr = function(value) {
	if (value < 10) {
		return "0" + value;
	}
	
	return value;
}