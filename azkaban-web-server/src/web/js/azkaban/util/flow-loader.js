/*
 * Copyright 2014 LinkedIn Corp.
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

var extendedViewPanels = {};
var extendedDataModels = {};
var openJobDisplayCallback = function(nodeId, flowId, evt) {
  console.log("Open up data");

  /*
  $("#flowInfoBase").before(cloneStuff);
  var requestURL = contextURL + "/manager";

  $.get(
      requestURL,
      {"project": projectName, "ajax":"fetchflownodedata", "flow":flowId, "node": nodeId},
      function(data) {
      var graphModel = new azkaban.GraphModel();
      graphModel.set({id: data.id, flow: data.flowData, type: data.type, props: data.props});

      var flowData = data.flowData;
      if (flowData) {
        createModelFromAjaxCall(flowData, graphModel);
      }

      var backboneView = new azkaban.FlowExtendedViewPanel({el:cloneStuff, model: graphModel});
      extendedViewPanels[nodeInfoPanelID] = backboneView;
      extendedDataModels[nodeInfoPanelID] = graphModel;
      backboneView.showExtendedView(evt);
      },
      "json"
    );
    */
}

var createNewPanel = function(node, model, evt) {
  var parentPath = node.parentPath;

  var nodeInfoPanelID = parentPath ? parentPath + ":" + node.id + "-info" : node.id + "-info";
  var cloneStuff = $("#flowInfoBase").clone();
  cloneStuff.data = node;
  $(cloneStuff).attr("id", nodeInfoPanelID);
  $("#flowInfoBase").before(cloneStuff);

  var backboneView = new azkaban.FlowExtendedViewPanel({el:cloneStuff, model: model});
  node.panel = backboneView;
  backboneView.showExtendedView(evt);
}

var closeAllSubDisplays = function() {
  $(".flowExtendedView").hide();
}

var nodeClickCallback = function(event, model, node) {
  console.log("Node clicked callback");

  var target = event.currentTarget;
  var type = node.type;
  var flowId = node.parent.flow;
  var jobId = node.id;

  var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + jobId;
  var menu = [];

  if (type == "flow") {
    var flowRequestURL = contextURL + "/manager?project=" + projectName + "&flow=" + node.flowId;
    if (node.expanded) {
      menu = [{title: "Collapse Flow...", callback: function() {model.trigger("collapseFlow", node);}}];
    }
    else {
      menu = [{title: "Expand Flow...", callback: function() {model.trigger("expandFlow", node);}}];
    }

    $.merge(menu, [
    //  {title: "View Properties...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
      {break: 1},
      {title: "Open Flow...", callback: function() {window.location.href=flowRequestURL;}},
      {title: "Open Flow in New Window...", callback: function() {window.open(flowRequestURL);}},
      {break: 1},
      {title: "Open Properties...", callback: function() {window.location.href=requestURL;}},
      {title: "Open Properties in New Window...", callback: function() {window.open(requestURL);}},
      {break: 1},
      {title: "Center Flow", callback: function() {model.trigger("centerNode", node);}}
    ]);
  }
  else {
    menu = [
    //  {title: "View Properties...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
    //  {break: 1},
      {title: "Open Job...", callback: function() {window.location.href=requestURL;}},
      {title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
      {break: 1},
      {title: "Center Job", callback: function() {model.trigger("centerNode", node)}}
    ];
  }
  contextMenuView.show(event, menu);
}

var jobClickCallback = function(event, model, node) {
  console.log("Node clicked callback");
  var target = event.currentTarget;
  var type = node.type;
  var flowId = node.parent.flow;
  var jobId = node.id;

  var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId + "&job=" + node.id;

  var menu;
  if (type == "flow") {
    var flowRequestURL = contextURL + "/manager?project=" + projectName + "&flow=" + node.flowId;
    menu = [
    //  {title: "View Properties...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
    //  {break: 1},
      {title: "Open Flow...", callback: function() {window.location.href=flowRequestURL;}},
      {title: "Open Flow in New Window...", callback: function() {window.open(flowRequestURL);}},
      {break: 1},
      {title: "Open Properties...", callback: function() {window.location.href=requestURL;}},
      {title: "Open Properties in New Window...", callback: function() {window.open(requestURL);}},
      {break: 1},
      {title: "Center Flow", callback: function() {model.trigger("centerNode", node)}}
    ];
  }
  else {
    menu = [
    //  {title: "View Job...", callback: function() {openJobDisplayCallback(jobId, flowId, event)}},
    //  {break: 1},
      {title: "Open Job...", callback: function() {window.location.href=requestURL;}},
      {title: "Open Job in New Window...", callback: function() {window.open(requestURL);}},
      {break: 1},
      {title: "Center Job", callback: function() {graphModel.trigger("centerNode", node)}}
    ];
  }
  contextMenuView.show(event, menu);
}

var edgeClickCallback = function(event, model) {
  console.log("Edge clicked callback");
}

var graphClickCallback = function(event, model) {
  console.log("Graph clicked callback");
  var data = model.get("data");
  var flowId = data.flow;
  var requestURL = contextURL + "/manager?project=" + projectName + "&flow=" + flowId;

  var menu = [
    {title: "Open Flow...", callback: function() {window.location.href=requestURL;}},
    {title: "Open Flow in New Window...", callback: function() {window.open(requestURL);}},
    {break: 1},
    {title: "Center Graph", callback: function() {model.trigger("resetPanZoom");}}
  ];

  contextMenuView.show(event, menu);
}

