Request: curl -X GET "http://localhost:8081/executions/2?session.id=<>"

Response:
{
  "executionId": 2,
  "submitUser": "azkaban",
  "submitTime": 1581383285656,
  "experimentId": "none",
  "concurrentOption": "skip",
  "failureAction": "FINISH_CURRENTLY_RUNNING",
  "rootExecutableNode": {
    "nodeId": "flowPriority2",
    "startTime": 1581383286307,
    "endTime": 1581383587055,
    "updateTime": 1581383587061,
    "nodeType": null,
    "condition": null,
    "nestedId": "flowPriority2",
    "inputNodeIds": [],
    "baseFlowId": "flowPriority2",
    "nodeList": [
      {
        "nodeId": "jobD",
        "startTime": 1581383436938,
        "endTime": 1581383436965,
        "updateTime": 1581383436970,
        "nodeType": "command",
        "condition": null,
        "nestedId": "jobD",
        "inputNodeIds": [
          "emb_1",
          "emb_2"
        ],
        "baseFlowId": null,
        "nodeList": null
      },
      {
        "nodeId": "jobE",
        "startTime": 1581383436979,
        "endTime": 1581383587011,
        "updateTime": 1581383587033,
        "nodeType": "command",
        "condition": null,
        "nestedId": "jobE",
        "inputNodeIds": [
          "jobD"
        ],
        "baseFlowId": null,
        "nodeList": null
      },
      {
        "nodeId": "emb_1",
        "startTime": 1581383286307,
        "endTime": 1581383286581,
        "updateTime": 1581383286581,
        "nodeType": "flow",
        "condition": null,
        "nestedId": "emb_1",
        "inputNodeIds": [],
        "baseFlowId": "flowPriority2:emb_1",
        "nodeList": [
          {
            "nodeId": "jobE1B",
            "startTime": 1581383286531,
            "endTime": 1581383286560,
            "updateTime": 1581383286566,
            "nodeType": "noop",
            "condition": null,
            "nestedId": "emb_1:jobE1B",
            "inputNodeIds": [
              "jobE1A"
            ],
            "baseFlowId": null,
            "nodeList": null
          },
          {
            "nodeId": "jobE1A",
            "startTime": 1581383286402,
            "endTime": 1581383286494,
            "updateTime": 1581383286507,
            "nodeType": "command",
            "condition": null,
            "nestedId": "emb_1:jobE1A",
            "inputNodeIds": [],
            "baseFlowId": null,
            "nodeList": null
          }
        ]
      },
      {
        "nodeId": "emb_2",
        "startTime": 1581383286400,
        "endTime": 1581383436933,
        "updateTime": 1581383436933,
        "nodeType": "flow",
        "condition": null,
        "nestedId": "emb_2",
        "inputNodeIds": [],
        "baseFlowId": "flowPriority2:emb_2",
        "nodeList": [
          {
            "nodeId": "jobE2C",
            "startTime": 1581383436888,
            "endTime": 1581383436922,
            "updateTime": 1581383436927,
            "nodeType": "noop",
            "condition": null,
            "nestedId": "emb_2:jobE2C",
            "inputNodeIds": [
              "emb_3"
            ],
            "baseFlowId": null,
            "nodeList": null
          },
          {
            "nodeId": "jobE2B",
            "startTime": 1581383286516,
            "endTime": 1581383286541,
            "updateTime": 1581383286549,
            "nodeType": "noop",
            "condition": null,
            "nestedId": "emb_2:jobE2B",
            "inputNodeIds": [
              "jobE2A"
            ],
            "baseFlowId": null,
            "nodeList": null
          },
          {
            "nodeId": "emb_3",
            "startTime": 1581383286560,
            "endTime": 1581383436881,
            "updateTime": 1581383436881,
            "nodeType": "flow",
            "condition": null,
            "nestedId": "emb_2:emb_3",
            "inputNodeIds": [
              "jobE2B"
            ],
            "baseFlowId": "flowPriority2:emb_2:emb_3",
            "nodeList": [
              {
                "nodeId": "emb_5",
                "startTime": 1581383286642,
                "endTime": 1581383286791,
                "updateTime": 1581383286791,
                "nodeType": "flow",
                "condition": null,
                "nestedId": "emb_2:emb_3:emb_5",
                "inputNodeIds": [
                  "jobE3A"
                ],
                "baseFlowId": "flowPriority2:emb_2:emb_3:emb_5",
                "nodeList": [
                  {
                    "nodeId": "jobE5B",
                    "startTime": 1581383286741,
                    "endTime": 1581383286776,
                    "updateTime": 1581383286781,
                    "nodeType": "command",
                    "condition": null,
                    "nestedId": "emb_2:emb_3:emb_5:jobE5B",
                    "inputNodeIds": [
                      "jobE5A"
                    ],
                    "baseFlowId": null,
                    "nodeList": null
                  },
                  {
                    "nodeId": "jobE5A",
                    "startTime": 1581383286656,
                    "endTime": 1581383286719,
                    "updateTime": 1581383286727,
                    "nodeType": "command",
                    "condition": null,
                    "nestedId": "emb_2:emb_3:emb_5:jobE5A",
                    "inputNodeIds": [],
                    "baseFlowId": null,
                    "nodeList": null
                  }
                ]
              },
              {
                "nodeId": "jobE3C",
                "startTime": 1581383436844,
                "endTime": 1581383436868,
                "updateTime": 1581383436874,
                "nodeType": "command",
                "condition": null,
                "nestedId": "emb_2:emb_3:jobE3C",
                "inputNodeIds": [
                  "jobE3B"
                ],
                "baseFlowId": null,
                "nodeList": null
              },
              {
                "nodeId": "jobE3B",
                "startTime": 1581383436798,
                "endTime": 1581383436828,
                "updateTime": 1581383436833,
                "nodeType": "command",
                "condition": null,
                "nestedId": "emb_2:emb_3:jobE3B",
                "inputNodeIds": [
                  "emb_5",
                  "emb_4"
                ],
                "baseFlowId": null,
                "nodeList": null
              },
              {
                "nodeId": "jobE3A",
                "startTime": 1581383286571,
                "endTime": 1581383286610,
                "updateTime": 1581383286627,
                "nodeType": "command",
                "condition": null,
                "nestedId": "emb_2:emb_3:jobE3A",
                "inputNodeIds": [],
                "baseFlowId": null,
                "nodeList": null
              },
              {
                "nodeId": "emb_4",
                "startTime": 1581383286632,
                "endTime": 1581383436789,
                "updateTime": 1581383436789,
                "nodeType": "flow",
                "condition": null,
                "nestedId": "emb_2:emb_3:emb_4",
                "inputNodeIds": [
                  "jobE3A"
                ],
                "baseFlowId": "flowPriority2:emb_2:emb_3:emb_4",
                "nodeList": [
                  {
                    "nodeId": "jobE4B",
                    "startTime": 1581383286722,
                    "endTime": 1581383436767,
                    "updateTime": 1581383436781,
                    "nodeType": "command",
                    "condition": null,
                    "nestedId": "emb_2:emb_3:emb_4:jobE4B",
                    "inputNodeIds": [
                      "jobE4A"
                    ],
                    "baseFlowId": null,
                    "nodeList": null
                  },
                  {
                    "nodeId": "jobE4A",
                    "startTime": 1581383286642,
                    "endTime": 1581383286675,
                    "updateTime": 1581383286687,
                    "nodeType": "command",
                    "condition": null,
                    "nestedId": "emb_2:emb_3:emb_4:jobE4A",
                    "inputNodeIds": [],
                    "baseFlowId": null,
                    "nodeList": null
                  }
                ]
              }
            ]
          },
          {
            "nodeId": "jobE2A",
            "startTime": 1581383286414,
            "endTime": 1581383286494,
            "updateTime": 1581383286504,
            "nodeType": "command",
            "condition": null,
            "nestedId": "emb_2:jobE2A",
            "inputNodeIds": [],
            "baseFlowId": null,
            "nodeList": null
          }
        ]
      }
    ]
  },
  "notifyFailureFirst": false,
  "notifyFailureLast": false
}
