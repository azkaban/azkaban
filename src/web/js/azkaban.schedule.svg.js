$.namespace('azkaban');

$(function() {


	var border = 20;
	var header = 30;
	var timeWidth = 60;
	var lineHeight = 40;
	var numDays = 7;
	var today = new Date();
	var totalHeight = (border * 2 + header + 24 * lineHeight);
	var monthConst = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
	var dayOfWeekConst = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];

	$("#svgDivCustom").svg({onLoad:
		function (svg) {

			var totalWidth = $("#svgDivCustom").width();

			$("#svgDivCustom").find("svg").eq(0).removeAttr("width");

			var dayWidth = Math.floor((totalWidth - 3 * border - timeWidth) / numDays);


			//Outer g
			var gMain = svg.group({transform: "translate(" + border + ".5," + border + ".5)", stroke : "#999", strokeWidth: 1});
			var defaultDate = new Date(today.setDate(today.getDate() - today.getDay()));
			today = new Date();
			var svgDate = defaultDate;

			//Used to filter projects or flows out
			var filterProject = new Array();
			var filterFlow = new Array();

			$(".nav-prev-week").click(function (event) {
				svgDate = new Date(svgDate.valueOf() - 7 * 24 *  60 * 60 * 1000);
				loadSvg(svgDate);
				event.stopPropagation();
			});
			$(".nav-next-week").click(function (event) {
				svgDate = new Date(svgDate.valueOf() + 7 * 24 *  60 * 60 * 1000);
				loadSvg(svgDate);
				event.stopPropagation();
			});
			$(".nav-this-week").click(function (event) {
				svgDate = defaultDate;
				loadSvg(svgDate);
				event.stopPropagation();
			});



			loadSvg(svgDate);

			function loadSvg(firstDay)
			{
				//svg.configure({viewBox: "0 0 " + totalWidth + " " + totalHeight, style: "width:100%"}, true);
				svg.remove(gMain);
				gMain = svg.group({transform: "translate(" + border + ".5," + border + ".5)", stroke : "#999", strokeWidth: 1});
				svg.text(gMain, timeWidth, header - 8, monthConst[firstDay.getMonth()], {fontSize: "20", style: "text-anchor: end;", fill : "#F60", stroke : "none"});
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
					var date = new Date(firstDay + 24*3600*1000 * deltaDay);
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

				if(isThisWeek != -1)
				{
					var date = new Date(firstDay + 24*3600*1000 * isThisWeek);
					var day = date.getDate();
					var gDay = svg.group(gMain, {transform: "translate(" + (border + timeWidth + isThisWeek * dayWidth) + "," + header + ")"});
					svg.rect(gDay, 0, -header, dayWidth, 24 * lineHeight + header, {fill : "none", stroke : "#06F"});
				}

				var gDayView = svg.group(gMain, {transform: "translate(" + (border + timeWidth) + "," + header + ")"});
				//A list of all items
				var itemByDay = new Array();
				for(var deltaDay = 0; deltaDay < numDays; deltaDay++) {
					itemByDay[deltaDay] = new Array();
				}

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
					gDayView = svg.group(gMain, {transform: "translate(" + (border + timeWidth) + "," + header + ")"});

					//Add day groups
					for(var deltaDay = 0; deltaDay < numDays; deltaDay++) {
						var gDay = svg.group(gDayView, {transform: "translate(" + (deltaDay * dayWidth) + ")"});
						var data = itemByDay[deltaDay];
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
							var gColumn = svg.group(gDay, {transform: "translate(" + (i * dayWidth / columns.length) + ")", style: "opacity: 0.8"});
							for(var j = 0; j < columns[i].length; j++) {
								//Draw item
								var item = columns[i][j];
								var startTime = new Date(parseInt(item.time));
								var startY = startTime.getHours() * lineHeight + startTime.getMinutes() * lineHeight / 60;
								var endTime = new Date(parseInt(item.time) + parseInt(item.length) );
								var endY = startY + parseInt(item.length)* lineHeight / 3600000;
								//var anchor = svg.a(gColumn);
								var itemUrl = contextURL + "/manager?project=" + item.projectname + "&flow=" + item.flowname;
								var gItem = svg.link(gColumn, itemUrl, {transform: "translate(0," + startY + ")"});
								$(gItem).data("item", item);
								//Replace the context handler
								$(gItem)[0].addEventListener('contextmenu', function(ev) {
									var requestURL = $(this).attr("href");
									var item = $(this).data("item");
									var menu = [
											{title: "Open Job...", callback: function() {window.location.href=requestURL;}},
											{title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
											{title: "Hide This Job", callback: function() {filterFlow.push(item); renderDays();}},
											{title: "Hide All Jobs from this Project", callback: function() {filterProject.push(item); renderDays();}}
									];
									contextMenuView.show(ev, menu);
									ev.preventDefault();
									ev.stopPropagation()
									return false;
								}, false);
								//$(gItem).attr("style","color:red");
								var rect = svg.rect(gItem, 0, 0, dayWidth / columns.length, endY - startY, 0, 0, {fill : "#7E7", stroke : "#444", strokeWidth : 1});
								//Draw text
								svg.text(gItem, 6, 16, item.flowname, {fontSize: "13", fill : "#000", stroke : "none"});
								//svg.text(gColumn, 6, startY + 32, "Project: " + item["projectname"], {fontSize: "13", fill : "#000", stroke : "none"});
							}
						}
					}
				}

				function addItem(item)
				{
					var itemStartTime = new Date(parseInt(item.time));
					var itemEndTime = new Date(parseInt(item.time) + parseInt(item.length));
					var itemStartDate = new Date(itemStartTime.getFullYear(), itemStartTime.getMonth(), itemStartTime.getDate());
					var itemEndDate = new Date(itemEndTime.getFullYear(), itemEndTime.getMonth(), itemEndTime.getDate());

					//Cross date item, cut it to only today's portion and add another item starting tomorrow morning
					if(itemStartDate.valueOf() != itemEndDate.valueOf() && itemEndTime.valueOf() != itemEndDate.valueOf())
					{
						var nextMorning = itemStartDate.valueOf() + 24*3600*1000;
						var excess = item.length - (nextMorning - itemStartTime.valueOf());
						item.length = (nextMorning - itemStartTime.valueOf()).toString();
						var item2 = {time: nextMorning.toString(), length: excess.toString(), projectname: item.projectname, flowname: item.flowname};
						addItem(item2);
					}

					//Now the item should be only in one day
					var index = (itemStartDate.valueOf() - firstDay) / (24*3600*1000);
					if(index >= 0 && index < numDays)
					{
						itemByDay[index].push(item);
					}
				}

				var requestURL = contextURL + "/schedule";

				for(var deltaDay = 0; deltaDay < numDays; deltaDay++)
				{
					//Asynchronously load data
					$.ajax({
						type: "GET",
						url: requestURL,
						data: {"ajax": "loadFlow", "day": firstDay + 24*3600*1000 * deltaDay, "loadPrev": deltaDay},
						dataType: "json",
						success: jQuery.proxy(function (data)
						{
							var items = data.items;

							//Sort items by day
							for(var i = 0; i < items.length; i++)
							{
								addItem(items[i]);
							}
							//Trigger a re-rendering of all the data
							renderDays();
						})
					});
				}
			}
		}, settings : {
			"xmlns" : "http://www.w3.org/2000/svg", 
			"xmlns:xlink" : "http://www.w3.org/1999/xlink", 
			"shape-rendering" : "optimize-speed",
			"style" : "width:100%;height:" + totalHeight + "px"
		}});

	function dayMatch(d1, d2)
	{
		return d1.getDate() == d2.getDate() && d1.getFullYear() == d2.getFullYear() && d1.getMonth() == d2.getMonth();
	}

	function getHourText(hour)
	{
		return (hour==0 ? "12 AM" : (hour<12 ? hour + " AM" : (hour==12 ? "12 PM" : (hour-12) + " PM" )));
	}

	function intersectArray(a, arry)
	{
		for(var i = 0; i < arry.length; i++)
		{
			if(intersects(a, arry[i]))
			{
				return true;
			}
		}

		return false;
	}

	function intersects(a, b)
	{
		return parseInt(a.time) < parseInt(b.time) + parseInt(b.length) && parseInt(a.time) + parseInt(a.length) > parseInt(b.time);
	}
});