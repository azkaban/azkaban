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

$.namespace('azkaban');

azkaban.GraphModel = Backbone.Model.extend({
  initialize: function() {

  },

  /*
   * Process and add data from JSON.
   */
  addFlow: function(data) {
    this.processFlowData(data);
    this.set({'data': data});
  },

  processFlowData: function(data) {
    var nodes = {};
    var edges = new Array();

    // Create a node map
    for (var i = 0; i < data.nodes.length; ++i) {
      var node = data.nodes[i];
      nodes[node.id] = node;
      if (!node.status) {
        node.status = "READY";
      }
    }

    // Create each node in and out nodes. Create an edge list.
    for (var i = 0; i < data.nodes.length; ++i) {
      var node = data.nodes[i];
      if (node.in) {
        for (var j = 0; j < node.in.length; ++j) {
          var fromNode = nodes[node.in[j]];
          if (!fromNode.outNodes) {
            fromNode.outNodes = {};
          }
          if (!node.inNodes) {
            node.inNodes = {};
          }

          fromNode.outNodes[node.id] = node;
          node.inNodes[fromNode.id] = fromNode;
          edges.push({to: node.id, from: fromNode.id});
        }
      }
    }

    // Iterate over the nodes again. Parse the data if they're embedded flow data.
    // Assign each nodes to the parent flow data.
    for (var key in nodes) {
      var node = nodes[key];
      node.parent = data;
      if (node.type == "flow") {
        this.processFlowData(node);
      }
    }

    // Assign the node map and the edge list
    data.nodeMap = nodes;
    data.edges = edges;
  }
});
