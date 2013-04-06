var getDuration = function(startMs, endMs) {
	if (startMs) {
		if (endMs == null || endMs < startMs) {
			return "-";
		}
		
		var diff = endMs - startMs;
		return formatDuration(diff, false);
	}

	return "-";
}

var formatDuration = function(duration, millisecSig) {
	var diff = duration;
	var seconds = Math.floor(diff / 1000);
	
	if (seconds < 60) {
		if (millisecSig) {
			return (diff / 1000).toFixed(millisecSig) + " s";
		}
		else {
			return seconds + " sec";
		}
	}
	
	var mins = Math.floor(seconds / 60);
	seconds = seconds % 60;
	if (mins < 60) {
		return mins + "m " + seconds + "s";
	}

	var hours = Math.floor(mins / 60);
	mins = mins % 60;
	if (hours < 24) {
		return hours + "h " + mins + "m " + seconds + "s";
	}
	
	var days = Math.floor(hours / 24);
	hours = hours % 24;
	
	return days + "d " + hours + "h " + mins + "m";
}

var getDateFormat = function(date) {
	var year = date.getFullYear();
	var month = getTwoDigitStr(date.getMonth() + 1);
	var day = getTwoDigitStr(date.getDate());
	
	var hours = getTwoDigitStr(date.getHours());
	var minutes = getTwoDigitStr(date.getMinutes());
	var second = getTwoDigitStr(date.getSeconds());

	var datestring = year + "-" + month + "-" + day + "  " + hours + ":" + minutes + " " + second + "s";
	return datestring;
}

var getHourMinSec = function(date) {
	var hours = getTwoDigitStr(date.getHours());
	var minutes = getTwoDigitStr(date.getMinutes());
	var second = getTwoDigitStr(date.getSeconds());
	
	var timestring = hours + ":" + minutes + " " + second + "s";
	return timestring;
}

var getTwoDigitStr = function(value) {
	if (value < 10) {
		return "0" + value;
	}
	
	return value;
}