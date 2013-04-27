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

$(function() {
	$(".expandable").each(
		function(index, value) {
			// Find collapsable arrow
			var childArrow = $(value).children(".arrow").click(function(evt) {toggleExpandCollapse(evt);});
			var subList = $(value).children("ul");
			
			if ($(value).hasClass("expand")) {
				subList.show();
			}
			else {
				subList.hide();
			}
		}
	);
});
