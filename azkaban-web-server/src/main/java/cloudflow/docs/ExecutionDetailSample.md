{
  "executionId": "2",
  "submitUser": "azkaban",
  "submitTime": 1581383285656,
  "concurrentOption": "skip",
  "failureAction": "FINISH_CURRENTLY_RUNNING",
  "notifyOnFirstFailure": false,
  "notifyFailureOnExecutionComplete": false,
  "experimentId": "defaultExperimentId",
  "description": "defaultDescription",
  "previousFlowExecutionId": "defaultPreviousExecutionId",
  "rootFlow": {
    "name": "flowPriority2",
    "type": "ROOT_FLOW",
    "startTime": 1581383286307,
    "endTime": 1581383587055,
    "status": "SUCCEEDED",
    "flowInfo": {
      "flowId": "defaultId",
      "flowName": "flowPriority2",
      "flowVersion": "1"
    },
    "nodeList": [
      {
        "name": "jobD",
        "type": "JOB",
        "startTime": 1581383436938,
        "endTime": 1581383436965,
        "status": "SUCCEEDED"
      },
      {
        "name": "jobE",
        "type": "JOB",
        "startTime": 1581383436979,
        "endTime": 1581383587011,
        "status": "SUCCEEDED"
      },
      {
        "name": "emb_1",
        "type": "EMBEDDED_FLOW",
        "startTime": 1581383286307,
        "endTime": 1581383286581,
        "status": "SUCCEEDED",
        "flowInfo": {
          "flowId": "defaultId",
          "flowName": "flowPriority2:emb_1",
          "flowVersion": "1"
        },
        "nodeList": [
          {
            "name": "jobE1B",
            "type": "JOB",
            "startTime": 1581383286531,
            "endTime": 1581383286560,
            "status": "SUCCEEDED"
          },
          {
            "name": "jobE1A",
            "type": "JOB",
            "startTime": 1581383286402,
            "endTime": 1581383286494,
            "status": "SUCCEEDED"
          }
        ]
      },
      {
        "name": "emb_2",
        "type": "EMBEDDED_FLOW",
        "startTime": 1581383286400,
        "endTime": 1581383436933,
        "status": "SUCCEEDED",
        "flowInfo": {
          "flowId": "defaultId",
          "flowName": "flowPriority2:emb_2",
          "flowVersion": "1"
        },
        "nodeList": [
          {
            "name": "jobE2C",
            "type": "JOB",
            "startTime": 1581383436888,
            "endTime": 1581383436922,
            "status": "SUCCEEDED"
          },
          {
            "name": "jobE2B",
            "type": "JOB",
            "startTime": 1581383286516,
            "endTime": 1581383286541,
            "status": "SUCCEEDED"
          },
          {
            "name": "emb_3",
            "type": "EMBEDDED_FLOW",
            "startTime": 1581383286560,
            "endTime": 1581383436881,
            "status": "SUCCEEDED",
            "flowInfo": {
              "flowId": "defaultId",
              "flowName": "flowPriority2:emb_2:emb_3",
              "flowVersion": "1"
            },
            "nodeList": [
              {
                "name": "emb_5",
                "type": "EMBEDDED_FLOW",
                "startTime": 1581383286642,
                "endTime": 1581383286791,
                "status": "SUCCEEDED",
                "flowInfo": {
                  "flowId": "defaultId",
                  "flowName": "flowPriority2:emb_2:emb_3:emb_5",
                  "flowVersion": "1"
                },
                "nodeList": [
                  {
                    "name": "jobE5B",
                    "type": "JOB",
                    "startTime": 1581383286741,
                    "endTime": 1581383286776,
                    "status": "SUCCEEDED"
                  },
                  {
                    "name": "jobE5A",
                    "type": "JOB",
                    "startTime": 1581383286656,
                    "endTime": 1581383286719,
                    "status": "SUCCEEDED"
                  }
                ]
              },
              {
                "name": "jobE3C",
                "type": "JOB",
                "startTime": 1581383436844,
                "endTime": 1581383436868,
                "status": "SUCCEEDED"
              },
              {
                "name": "jobE3B",
                "type": "JOB",
                "startTime": 1581383436798,
                "endTime": 1581383436828,
                "status": "SUCCEEDED"
              },
              {
                "name": "jobE3A",
                "type": "JOB",
                "startTime": 1581383286571,
                "endTime": 1581383286610,
                "status": "SUCCEEDED"
              },
              {
                "name": "emb_4",
                "type": "EMBEDDED_FLOW",
                "startTime": 1581383286632,
                "endTime": 1581383436789,
                "status": "SUCCEEDED",
                "flowInfo": {
                  "flowId": "defaultId",
                  "flowName": "flowPriority2:emb_2:emb_3:emb_4",
                  "flowVersion": "1"
                },
                "nodeList": [
                  {
                    "name": "jobE4B",
                    "type": "JOB",
                    "startTime": 1581383286722,
                    "endTime": 1581383436767,
                    "status": "SUCCEEDED"
                  },
                  {
                    "name": "jobE4A",
                    "type": "JOB",
                    "startTime": 1581383286642,
                    "endTime": 1581383286675,
                    "status": "SUCCEEDED"
                  }
                ]
              }
            ]
          },
          {
            "name": "jobE2A",
            "type": "JOB",
            "startTime": 1581383286414,
            "endTime": 1581383286494,
            "status": "SUCCEEDED"
          }
        ]
      }
    ]
  }
}
