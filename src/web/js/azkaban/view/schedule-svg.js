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

$.namespace('azkaban');

$(function() {

  var border = 20;
  var header = 30;
  var minTimeWidth = 80;
  var timeWidth = minTimeWidth;
  var lineHeight = 40;
  var numDays = 7;
  var today = new Date();
  var totalHeight = (border * 2 + header + 24 * lineHeight);
  var monthConst = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
  var dayOfWeekConst = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
  var hourMillisConst = 3600 * 1000;
  var dayMillisConst = 24 * hourMillisConst;

  $("#svgDivCustom").svg({onLoad:
    function (svg) {

      var totalWidth = $("#svgDivCustom").width();

      $("#svgDivCustom").find("svg").eq(0).removeAttr("width");


      //Outer g
      var gMain = svg.group({transform: "translate(" + border + ".5," + border + ".5)", stroke : "#999", strokeWidth: 1});
      var defaultDate = new Date(today.setDate(today.getDate() - today.getDay()));
      today = new Date();
      var svgDate = defaultDate;

      //Load the date from the hash if existing
      if(window.location.hash) {
        try {
          var dateParts = window.location.hash.replace("#", "").split("-");
          var newDate = new Date(parseInt(dateParts[0]), parseInt(dateParts[1]) - 1, parseInt(dateParts[2]));
          if(!isNaN(newDate)) {
            svgDate = newDate;
          }
        }
        catch(err){ }
      }

      //Used to filter projects or flows out
      var filterProject = new Array();
      var filterFlow = new Array();

      $(".nav-prev-week").click(function (event) {
        svgDate = new Date(svgDate.valueOf() - 7 * dayMillisConst);
        window.location.hash = "#" + svgDate.getFullYear() + "-" + (svgDate.getMonth() + 1) + "-" + svgDate.getDate();
        loadSvg(svgDate);
        event.stopPropagation();
      });
      $(".nav-next-week").click(function (event) {
        svgDate = new Date(svgDate.valueOf() + 7 * dayMillisConst);
        window.location.hash = "#" + svgDate.getFullYear() + "-" + (svgDate.getMonth() + 1) + "-" + svgDate.getDate();
        loadSvg(svgDate);
        event.stopPropagation();
      });
      $(".nav-this-week").click(function (event) {
        svgDate = defaultDate;
        window.location.hash = "#" + svgDate.getFullYear() + "-" + (svgDate.getMonth() + 1) + "-" + svgDate.getDate();
        loadSvg(svgDate);
        event.stopPropagation();
      });



      loadSvg(svgDate);

      function loadSvg(firstDay)
      {
        //Text to show which month/year it is
        var monthIndicatorText = monthConst[firstDay.getMonth()] + " " + firstDay.getFullYear().toString();
        //Measure a good width for the text to display well
        timeWidth = Math.max(minTimeWidth, measureText(svg, monthIndicatorText, {fontSize: "20", style: "text-anchor: end;"}));

        var dayWidth = Math.floor((totalWidth - 3 * border - timeWidth) / numDays);

        //svg.configure({viewBox: "0 0 " + totalWidth + " " + totalHeight, style: "width:100%"}, true);
        svg.remove(gMain);
        gMain = svg.group({transform: "translate(" + border + ".5," + border + ".5)", stroke : "#999", strokeWidth: 1});
        svg.text(gMain, timeWidth, header - 8, monthIndicatorText, {fontSize: "20", style: "text-anchor: end;", fill : "#F60", stroke : "none"});
        //time indicator group
        var gLeft = svg.group(gMain, {transform: "translate(0," + header + ")"});
        //Draw lines and hours
        for(var i = 0; i < 24; i++)
        {
          svg.line(gLeft, 0, i * lineHeight, timeWidth, i * lineHeight);
          //Gets the hour text from an integer from 0 to 23
          var hourText = getHourText(i);
          //Move text down a bit? TODO: Is there a CSS option for top anchor?
          svg.text(gLeft, timeWidth, i * lineHeight + 15, hourText, {fontSize: "14", style: "text-anchor: end;", fill : "#333", stroke : "none"});
        }

        //var firstDay = new Date();//(new Date()).valueOf();
        firstDay = new Date(firstDay.getFullYear(), firstDay.getMonth(), firstDay.getDate()).valueOf();
        var isThisWeek = -1;
        //Draw background
        for(var deltaDay = 0; deltaDay < numDays; deltaDay++)
        {
          //Day group
          var gDay = svg.group(gMain, {transform: "translate(" + (border + timeWidth + deltaDay * dayWidth) + "," + header + ")"});

          //This is temporary.
          var date = new Date(firstDay + dayMillisConst * deltaDay);
          var day = date.getDate();

          //Draw box around
          var isToday = date.getFullYear() == today.getFullYear() && date.getMonth() == today.getMonth() && date.getDate() == today.getDate();
          if(isToday)
          {
            isThisWeek = deltaDay;
          }
          svg.rect(gDay, 0, -header, dayWidth, 24 * lineHeight + header, {fill : "none", stroke : "#F60"});
          //Draw day title
          svg.text(gDay, 6, -8, day + " " + dayOfWeekConst[date.getDay()], {fontSize: "20", fill : isToday?"#06C":"#F60", stroke : "none"});

          //Draw horizontal lines
          for(var i = 0; i < 24; i++)
          {
            svg.line(gDay, 0, i * lineHeight, dayWidth, i * lineHeight);
          }
        }

        var gDayViewOuterGroup = svg.group(gMain);
        var gDayView = svg.group(gDayViewOuterGroup, {transform: "translate(" + (border + timeWidth) + "," + header + ")"});
        if(isThisWeek != -1)
        {
          var date = new Date(firstDay + dayMillisConst * isThisWeek);
          var day = date.getDate();
          var gDay = svg.group(gMain, {transform: "translate(" + (border + timeWidth + isThisWeek * dayWidth) + "," + header + ")"});
          svg.rect(gDay, 0, -header, dayWidth, 24 * lineHeight + header, {fill : "none", stroke : "#06F"});
          var lineY = Math.floor(today.getHours() * lineHeight + today.getMinutes() * lineHeight / 60);
          svg.line(gDay, 0, lineY, dayWidth, lineY, {fill : "none", stroke : "#06F", strokeWidth : 4});
        }

        //A list of all items
        var itemByDay = new Array();
        for(var deltaDay = 0; deltaDay < numDays; deltaDay++) {
          itemByDay[deltaDay] = new Array();
        }

        var itemByScheduleIdMap = {};

        function filterApplies(item) {
          for(var i = 0; i < filterProject.length; i++) {
            if(item.projectname == filterProject[i].projectname) {
              return true;
            }
          }
          for(var i = 0; i < filterFlow.length; i++) {
            if(item.projectname == filterFlow[i].projectname && item.flowname == filterFlow[i].flowname) {
              return true;
            }
          }
          return false;
        }

        //Function that re-renders all loaded items
        function renderDays() {
          //Clear items inside the day view
          svg.remove(gDayView);
          gDayView = svg.group(gDayViewOuterGroup, {transform: "translate(" + (border + timeWidth) + "," + header + ")"});

          //Add day groups
          for(var deltaDay = 0; deltaDay < numDays; deltaDay++) {
            var gDay = svg.group(gDayView, {transform: "translate(" + (deltaDay * dayWidth) + ")"});
            var data = itemByDay[deltaDay];
            //Sort the arrays to have a better view
            data.sort(function (a, b){
              //Smaller time in front
              var timeDiff = a.time - b.time;
              if(timeDiff == 0) {
                //Larger length in front
                var lengthDiff = b.length - a.length;
                if(lengthDiff == 0) {
                  //Sort by alphabetical
                  return (a.flowname < b.flowname ? 1 : a.flowname > b.flowname ? -1 : 0);
                }
                return lengthDiff;
              }
              return timeDiff;
            });
            //Sort items to columns
            var columns = new Array();
            columns.push(new Array());
            //Every item is parsed through here into columns
            for(var i = 0; i < data.length; i++) {
              //Apply filters here
              if(filterApplies(data[i])) {
                continue;
              }

              var foundColumn = false;
              //Go through every column until a place can be found
              for(var j = 0; j < columns.length; j++) {
                if(!intersectArray(data[i], columns[j])) {
                  //Found a place
                  columns[j].push(data[i]);
                  foundColumn = true;
                  break;
                }
              }
              //No place, create new column
              if(!foundColumn) {
                columns.push(new Array());
                columns[columns.length - 1].push(data[i]);
              }
            }

            //Actually drawing them
            for(var i = 0; i < columns.length; i++) {
              //Split into columns
              var gColumn = svg.group(gDay, {transform: "translate(" + Math.floor(i * dayWidth / columns.length) + ")", style: "opacity: 0.8"});
              for(var j = 0; j < columns[i].length; j++) {
                //Draw item
                var item = columns[i][j];
                var startTime = new Date(item.time);
                var startY = Math.floor(startTime.getHours() * lineHeight + startTime.getMinutes() * lineHeight / 60);
                var endTime = new Date(item.time + item.length );
                var endY = Math.ceil(startY + (item.length * lineHeight) / hourMillisConst);
                var deltaY = Math.ceil(endY - startY);
                if(deltaY < 5){
                  deltaY = 5;
                }
                //var anchor = svg.a(gColumn);
                var itemUrl = contextURL + "/manager?project=" + item.projectname + "&flow=" + item.flowname;
                var gItem = svg.link(gColumn, itemUrl, {transform: "translate(0," + startY + ")"});

                //Pass the item into the DOM data store to be retrieved later on
                $(gItem).data("item", item);

                //Replace the context handler
                gItem.addEventListener('contextmenu', handleContextMenu);

                //Add a tooltip on mouse over
                gItem.addEventListener('mouseover', handleMouseOver);
                //Remove the tooltip on mouse out
                gItem.addEventListener('mouseout', handleMouseOut);

                //$(gItem).attr("style","color:red");
                var rect = svg.rect(gItem, 0, 0, Math.ceil(dayWidth / columns.length), deltaY, 0, 0, {fill : item.item.color, stroke : "#444", strokeWidth : 1});

                item.rect = rect;
                //Draw text
                //svg.text(gItem, 6, 16, item.flowname, {fontSize: "13", fill : "#000", stroke : "none"});
              }
            }
          }
        }

        function processItem(item, scheduled)
        {
          var firstTime = item.time;
          var startTime = firstDay;
          var endTime = firstDay + numDays * dayMillisConst;
          var period = item.period;
          var restrictedStartTime = Math.max(firstDay, today.valueOf());
          if(!scheduled){
            restrictedStartTime = firstDay;
          }

          // Shift time until we're past the start time
          if (period > 0) {
            // Calculate next execution time efficiently
            // Take into account items that ends in the date specified, but does not start on that date
            var periods = Math.floor((restrictedStartTime - (firstTime)) / period);
            //Make sure we don't subtract
            if(periods < 0){
              periods = 0;
            }
            firstTime += period * periods;
            // Increment in case we haven't arrived yet. This will apply to most of the cases
            while (firstTime < restrictedStartTime) {
              firstTime += period;
            }
          }

          // Bad or no period
          if (period <= 0) {
            // Single instance case
            if (firstTime >= restrictedStartTime && firstTime < endTime) {
              addItem({scheduleid: item.scheduleid, flowname : item.flowname, projectname: item.projectname, time: firstTime, length: item.length, item: item});
            }
          }
          else {
            if(period <= hourMillisConst) {
              addItem({scheduleid: item.scheduleid, flowname : item.flowname, projectname: item.projectname, time: firstTime, length: endTime - firstTime, item: item});
            }
            else{
              // Repetitive schedule, firstTime is assumed to be after startTime
              while (firstTime < endTime) {
                addItem({scheduleid: item.scheduleid, flowname : item.flowname, projectname: item.projectname, time: firstTime, length: item.length, item: item});
                firstTime += period;
              }
            }
          }
        }

        function addItem(obj)
        {
          var itemStartTime = new Date(obj.time);
          var itemEndTime = new Date(obj.time + obj.length);
          var itemStartDate = new Date(itemStartTime.getFullYear(), itemStartTime.getMonth(), itemStartTime.getDate());
          var itemEndDate = new Date(itemEndTime.getFullYear(), itemEndTime.getMonth(), itemEndTime.getDate());

          //Cross date item, cut it to only today's portion and add another item starting tomorrow morning
          if(itemStartDate.valueOf() != itemEndDate.valueOf() && itemEndTime.valueOf() != itemStartDate + dayMillisConst)
          {
            var nextMorning = itemStartDate.valueOf() + dayMillisConst;
            var excess = obj.length - (nextMorning - itemStartTime.valueOf());
            obj.length = nextMorning - itemStartTime.valueOf();
            while(excess > 0)
            {
              var tempLength = excess;
              if(tempLength > dayMillisConst){
                tempLength = dayMillisConst;
              }

              var item2 = {scheduleid: obj.scheduleid, time: nextMorning, length: tempLength, projectname: obj.projectname, flowname: obj.flowname, item: obj.item};
              addItem(item2);
              excess -= tempLength;
              nextMorning += dayMillisConst;
            }
          }

          //Now the item should be only in one day
          var index = (itemStartDate.valueOf() - firstDay) / dayMillisConst;
          if(index >= 0 && index < numDays)
          {
            //Add the item to the rendering list
            itemByDay[index].push(obj);
            //obj.item.objs.push(obj);

            if(!itemByScheduleIdMap[obj.scheduleid]){
              itemByScheduleIdMap[obj.scheduleid] = new Array();
            }
            itemByScheduleIdMap[obj.scheduleid].push(obj);
          }
        }

        function handleContextMenu(event) {
          var requestURL = $(this).attr("href");
          var item = $(this).data("item");
          var menu = [
            {title: "Job \"" + item.flowname + "\" From Project \"" + item.projectname + "\""},
            {title: "View Job", callback: function() {window.location.href=requestURL;}},
            {title: "View Job in New Window", callback: function() {window.open(requestURL);}},
            {title: "Hide Job", callback: function() {filterFlow.push(item); renderDays();}},
            {title: "Hide All Jobs From the Same Project", callback: function() {filterProject.push(item); renderDays();}}
          ];
          contextMenuView.show(event, menu);
          event.preventDefault();
          event.stopPropagation();
          return false;
        }

        function handleMouseOver(event) {
          //Create the new tooltip
          var requestURL = $(this).attr("href");
          var obj = $(this).data("item");
          var offset = $("svg").offset();
          var thisOffset = $(this).offset();

          var tooltip = svg.group({transform: "translate(" + (thisOffset.left - offset.left + 2) + "," + (thisOffset.top - offset.top - 2) + ")"});
          var text = [
            "\"" + obj.flowname + "\" from \"" + obj.projectname + "\"",
            "Repeat: " + formatReadablePeriod(obj.item.period)
          ];

          if(obj.item.period == 0){
            text[1] = "";
            if(obj.item.history == true) {
              if(obj.item.status == 50){
                text[1] = "SUCCEEDED";
              }
              else if(obj.item.status == 60){
                text[1] = "KILLED";
              }
              else if(obj.item.status == 70){
                text[1] = "FAILED";
              }
              else if(obj.item.status == 80){
                text[1] = "FAILED_FINISHING";
              }
              else if(obj.item.status == 90){
                text[1] = "SKIPPED";
              }
            }
          }
          var textLength = Math.max(measureText(svg, text[0], {fontSize: "13"}), measureText(svg, text[1], {fontSize: "13"}));
          var rect = svg.rect(tooltip, 0, -40, textLength + 4, 40, {fill : "#FFF", stroke : "none"});
          svg.text(tooltip, 2, -25, text[0], {fontSize: "13", fill : "#000", stroke : "none"});
          svg.text(tooltip, 2, -5, text[1], {fontSize: "13", fill : "#000", stroke : "none"});

          //Store tooltip
          $(this).data("tooltip", tooltip);

          if(itemByScheduleIdMap[obj.scheduleid]){
            //Item highlight effect
            var arry = itemByScheduleIdMap[obj.scheduleid];
            for(var i = 0; i < arry.length; i++) {
              $(arry[i].rect).attr("fill", "#FF0");
            }
          }
        }

        function handleMouseOut(event) {
          //Item highlight effect
          var obj = $(this).data("item");
            //Item highlight effect
          if(itemByScheduleIdMap[obj.scheduleid]){
            var arry = itemByScheduleIdMap[obj.scheduleid];
            for(var i = 0; i < arry.length; i++) {
              var obj2 = obj.item.objs[i];
              $(arry[i].rect).attr("fill", arry[i].item.color);
            }
          }
          //Clear the fade interval
          $($(this).data("tooltip")).fadeOut(250, function(){ svg.remove(this); });
        }

        //Asynchronously load data
        var requestURL = contextURL + "/schedule";
        $.ajax({
          type: "GET",
          url: requestURL,
          data: {"ajax": "loadFlow"},
          dataType: "json",
          success: function (data)
          {
            var items = data.items;

            //Sort items by day
            for(var i = 0; i < items.length; i++)
            {
              //items[i].length = hourMillisConst; //TODO: Remove this to get the actual length
              items[i].objs = new Array();
              items[i].color = "#69F";
              processItem(items[i], true);
            }
            //Trigger a re-rendering of all the data
            renderDays();
          }
        });
        for(var deltaDay = 0; deltaDay < numDays; deltaDay++) {
          $.ajax({
            type: "GET",
            url: requestURL,
            data: {"ajax": "loadHistory", "startTime": firstDay + deltaDay * dayMillisConst, "loadAll" : 0},
            //dataType: "json",
            success: function (data)
            {
              var items = data.items;

              //Sort items by day
              for(var i = 0; i < items.length; i++)
              {
                //if(items[i].length < 5 * 60 * 1000) items[i].length = 5 * 60 * 1000;
                items[i].objs = new Array();
                items[i].color = "#7E7";
                if(items[i].status == 60 || items[i].status == 70 || items[i].status == 80)
                  items[i].color = "#E77";
                processItem(items[i], false);
              }
              //Trigger a re-rendering of all the data
              renderDays();
            }
          });
        }
      }
    }, settings : {
      "xmlns" : "http://www.w3.org/2000/svg",
      "xmlns:xlink" : "http://www.w3.org/1999/xlink",
      "shape-rendering" : "optimize-speed",
      "style" : "width:100%;height:" + totalHeight + "px"
    }});

  function dayMatch(d1, d2) {
    return d1.getDate() == d2.getDate() && d1.getFullYear() == d2.getFullYear() && d1.getMonth() == d2.getMonth();
  }

  function getHourText(hour) {
    return (hour==0 ? "12 AM" : (hour<12 ? hour + " AM" : (hour==12 ? "12 PM" : (hour-12) + " PM" )));
  }

  function intersectArray(a, arry) {
    for(var i = 0; i < arry.length; i++) {
      var b = arry[i];
      if(a.time < b.time + b.length && a.time + a.length > b.time) {
        return true;
      }
    }

    return false;
  }

  function measureText(svg, text, options) {
    var test = svg.text(0, 0, text, options);
    var width = test.getComputedTextLength();
    svg.remove(test);
    return width;
  }

  function formatReadablePeriod(period) {
    var days = Math.floor(period / dayMillisConst);
    var hour = period - days * dayMillisConst;
    var hours = Math.floor(hour / hourMillisConst);
    var min = hour - hours * hourMillisConst;
    var mins = Math.floor(min / 60000);

    var text = "";
    if(days > 0) text = (days == 1 ? "24 hours" : days.toString() + " days");
    if(hours > 0) text = text + " " + (hours == 1 ? "1 hour" : hours.toString() + " hours");
    if(mins > 0) text = text + " " + (mins == 1 ? "1 minute" : mins.toString() + " minutes");
    return text;
  }
});
