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

var getDuration = function (startMs, endMs) {
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

var formatDuration = function (duration, millisecSig) {
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

var getDateFormat = function (date) {
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

var getHourMinSec = function (date) {
  var hours = getTwoDigitStr(date.getHours());
  var minutes = getTwoDigitStr(date.getMinutes());
  var second = getTwoDigitStr(date.getSeconds());

  var timestring = hours + ":" + minutes + " " + second + "s";
  return timestring;
}

var getTwoDigitStr = function (value) {
  if (value < 10) {
    return "0" + value;
  }

  return value;
}

// Verify if a cron String meets the requirement of Quartz Cron Expression.
var validateQuartzStr = function (str) {
  var res = str.split(" "), len = res.length;

  // A valid Quartz Cron Expression should have 6 or 7 fields.
  if (len < 6 || len >= 8) {
    return "NUM_FIELDS_ERROR";
  }

  // Quartz currently doesn't support specifying both a day-of-week and a day-of-month value
  // (you must currently use the ‘?’ character in one of these fields).
  // http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html#notes
  if (res[3] != '?' && res[5] != '?') {
    return "DOW_DOM_STAR_ERROR";
  }

  //valid string
  return "VALID";
}

// Users enter values 1-7 for day of week, but UnixCronSyntax requires
// day of week to be values 0-6. However, when using "#" syntax, we
// do not want to apply the modulo operation to the number following "#"
var modifyStrToUnixCronSyntax = function (str) {
  var res = str.split("#");
  res[0] = res[0].replace(/[0-7]/g, function upperToHyphenLower(match) {
    return (parseInt(match) + 6) % 7;
  });
  return res.join("#");
}

// Unix Cron use 0-6 as Sun--Sat, but Quartz use 1-7. Due to later.js only supporting Unix Cron, we have to make this transition.
// The detailed Unix Cron Syntax: https://en.wikipedia.org/wiki/Cron
// The input is a 5 field string (without year) or 6 field String (with year).
var transformFromQuartzToUnixCron = function (str) {
  var res = str.split(" ");

  // If the cron doesn't include year field
  if (res.length == 5) {
    res[res.length - 1] = modifyStrToUnixCronSyntax(res[res.length - 1]);
  }// If the cron Str does include year field
  else if (res.length == 6) {
    res[res.length - 2] = modifyStrToUnixCronSyntax(res[res.length - 2]);
  }

  return res.join(" ");
}
