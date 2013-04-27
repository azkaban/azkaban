$(function() {
	// For page selection highlighter
	var page = $("#page");
	if (page.length) {
		var pageId = page.attr("pageid");
		if (pageId) {
			var nav = $("#nav-" + pageId);
			if (nav.length) {
				nav.addClass("selected");
			}
		}
	}
});
