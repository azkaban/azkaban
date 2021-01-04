// Common functions for rendering a job list

function atLeastOneChildHasChildren(node) {
  return node.nodes && node.nodes.reduce((acc, x) => acc || (x.nodes && x.nodes.length), false);
}

function createExpandAllButton() {
  var expandAllDiv = document.createElement("div");
  $(expandAllDiv).addClass("expandallarrow");
  var firstArrow = document.createElement("span");
  $(firstArrow).addClass("glyphicon glyphicon-chevron-down");
  var secondArrow = document.createElement("span");
  $(secondArrow).addClass("glyphicon glyphicon-chevron-down");
  $(expandAllDiv).append(firstArrow);
  $(expandAllDiv).append(secondArrow);
  return expandAllDiv;
}