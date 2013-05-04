function toggleExpandCollapse(evt) {
	var arrow = evt.currentTarget;
	var parent = arrow.parentElement;
	if ($(parent).hasClass("expand")) {
		// Collapse
		$(parent).removeClass("expand")
		$(parent).children("ul").slideUp("fast");
	}
	else {
		// Expand
		$(parent).addClass("expand")
		$(parent).children("ul").slideDown("fast");
	}
	
}

function expandAndRecurse(ul) {
	if ($(ul).hasClass("root")) {
		return;
	}
	$(ul).show();
	if ($(ul).parent()[0].tagName == "LI") {
		$(ul).parent().addClass("expand");
		expandAndRecurse($(value).parent().parent());
	}
}

$(function() {
	$(".expandable").each(
		function(index, value) {
			// Find collapsable arrow
			var childArrow = $(value).children(".arrow").click(function(evt) {toggleExpandCollapse(evt);});
			var subList = $(value).children("ul");
			subList.hide();
		}
	);
	
	$(".selected").children().show();
	expandAndRecurse($(".selected").parent());
});
