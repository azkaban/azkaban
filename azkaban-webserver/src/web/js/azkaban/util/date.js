/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

var TIMESTAMP_LENGTH = 13;

var getDuration = function(startMs, endMs) {
  if (startMs) {
    if (startMs == -1) {
      return "-";
    }
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

  var datestring = year + "-" + month + "-" + day + "  " + hours + ":" +
      minutes + " " + second + "s";
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
