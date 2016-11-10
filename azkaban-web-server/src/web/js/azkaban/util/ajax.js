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

function ajaxCall(requestURL, data, callback) {
  var successHandler = function(data) {
    if (data.error == "session") {
      // We need to relogin.
      var errorDialog = document.getElementById("invalid-session");
      if (errorDialog) {
        $(errorDialog).modal({
          closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
          position: ["20%",],
          containerId: 'confirm-container',
          containerCss: {
            'height': '220px',
            'width': '565px'
          },
          onClose: function (dialog) {
            window.location.reload();
          }
        });
      }
    }
    else {
      callback.call(this,data);
    }
  };
  $.get(requestURL, data, successHandler, "json");
}

function executeFlow(executingData) {
  executeURL = contextURL + "/executor";
  var successHandler = function(data) {
    if (data.error) {
      flowExecuteDialogView.hideExecutionOptionPanel();
      messageDialogView.show("Error Executing Flow", data.error);
    }
    else {
      flowExecuteDialogView.hideExecutionOptionPanel();
      messageDialogView.show("Flow submitted", data.message,
        function() {
          var redirectURL = contextURL + "/executor?execid=" + data.execid;
          window.location.href = redirectURL;
        }
      );
    }
  };

  $.get(executeURL, executingData, successHandler, "json");
}

function fetchFlowInfo(model, projectName, flowId, execId) {
  var fetchData = {"project": projectName, "ajax":"flowInfo", "flow":flowId};
  if (execId) {
    fetchData.execid = execId;
  }

  var executeURL = contextURL + "/executor";
  var successHandler = function(data) {
    if (data.error) {
      alert(data.error);
    }
    else {
      model.set({
        "successEmails": data.successEmails,
        "failureEmails": data.failureEmails,
        "failureAction": data.failureAction,
        "notifyFailure": {
          "first": data.notifyFailureFirst,
          "last": data.notifyFailureLast
        },
        "flowParams": data.flowParam,
        "isRunning": data.running,
        "nodeStatus": data.nodeStatus,
        "concurrentOption": data.concurrentOptions,
        "pipelineLevel": data.pipelineLevel,
        "pipelineExecution": data.pipelineExecution,
        "queueLevel":data.queueLevel
      });
    }
    model.trigger("change:flowinfo");
  };

  $.ajax({
    url: executeURL,
    data: fetchData,
    success: successHandler,
    dataType: "json",
    async: false
  });
}

function fetchFlow(model, projectName, flowId, sync) {
  // Just in case people don't set sync
  sync = sync ? true : false;
  var managerUrl = contextURL + "/manager";
  var fetchData = {
    "ajax" : "fetchflowgraph",
    "project" : projectName,
    "flow" : flowId
  };
  var successHandler = function(data) {
    if (data.error) {
      alert(data.error);
    }
    else {
      var disabled = data.disabled ? data.disabled : {};
      model.set({
        flowId: data.flowId,
        data: data,
        disabled: disabled
      });

      var nodeMap = {};
      for (var i = 0; i < data.nodes.length; ++i) {
        var node = data.nodes[i];
        nodeMap[node.id] = node;
      }

      for (var i = 0; i < data.edges.length; ++i) {
         var edge = data.edges[i];

         if (!nodeMap[edge.target].in) {
          nodeMap[edge.target].in = {};
         }
         var targetInMap = nodeMap[edge.target].in;
         targetInMap[edge.from] = nodeMap[edge.from];

         if (!nodeMap[edge.from].out) {
          nodeMap[edge.from].out = {};
         }
         var sourceOutMap = nodeMap[edge.from].out;
         sourceOutMap[edge.target] = nodeMap[edge.target];
      }

      model.set({nodeMap: nodeMap});
    }
  };

  $.ajax({
    url: managerUrl,
    data: fetchData,
    success: successHandler,
    dataType: "json",
    async: !sync
  });
}

/**
* Checks to see if a flow is running.
*
*/
function flowExecutingStatus(projectName, flowId) {
  var requestURL = contextURL + "/executor";

  var executionIds;
  var successHandler = function(data) {
    if (data.error == "session") {
      // We need to relogin.
      var errorDialog = document.getElementById("invalid-session");
      if (errorDialog) {
        $(errorDialog).modal({
          closeHTML: "<a href='#' title='Close' class='modal-close'>x</a>",
          position: ["20%",],
          containerId: 'confirm-container',
          containerCss: {
            'height': '220px',
            'width': '565px'
          },
          onClose: function (dialog) {
            window.location.reload();
          }
        });
      }
    }
    else {
      executionIds = data.execIds;
    }
  };
  $.ajax({
    url: requestURL,
    async: false,
    data: {
      "ajax": "getRunning",
      "project": projectName,
      "flow": flowId
    },
    error: function(data) {},
    success: successHandler
  });

  return executionIds;
}
