function hasClass(el, name) {
  var classes = el.getAttribute("class");
  if (classes == null) {
    return false;
  }
  return new RegExp('(\\s|^)'+name+'(\\s|$)').test(classes);
}

function addClass(el, name) {
  if (!hasClass(el, name)) {
    var classes = el.getAttribute("class");
    if (classes) {
      classes += ' ' + name;
    }
    else {
      classes = name;
    }
    el.setAttribute("class", classes);
  }
}

function removeClass(el, name) {
  if (hasClass(el, name)) {
    var classes = el.getAttribute("class");
    el.setAttribute("class", classes.replace(new RegExp('(\\s|^)'+name+'(\\s|$)'),' ').replace(/^\s+|\s+$/g, ''));
  }
}

function translateStr(x, y) {
  return "translate(" + x + "," + y + ")";
}

function animatePolylineEdge(svg, edge, newPoints, time) {
  var oldEdgeGuides = edge.oldpoints;

  var interval = 10;
  var numsteps = time/interval;

  var deltaEdges = new Array();
  for (var i=0; i < oldEdgeGuides.length; ++i) {
    var startPoint = oldEdgeGuides[i];
    var endPoint = newPoints[i];

    var deltaX = (endPoint[0] - startPoint[0])/numsteps;
    var deltaY = (endPoint[1] - startPoint[1])/numsteps;
    deltaEdges.push([deltaX, deltaY]);
  }

  animatePolyLineLoop(svg, edge, oldEdgeGuides, deltaEdges, numsteps, 25);
}

function animatePolyLineLoop(svg, edge, lastPoints, deltaEdges, step, time) {
  for (var i=0; i < deltaEdges.length; ++i) {
    lastPoints[i][0] += deltaEdges[i][0];
    lastPoints[i][1] += deltaEdges[i][1];
  }

  svg.change(edge.line, {points: lastPoints});
  if (step > 0) {
    setTimeout(
      function(){
        animatePolyLineLoop(svg, edge, lastPoints, deltaEdges, step - 1);
      },
      time
    );
  }
}
