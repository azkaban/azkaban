1. Create the tables using the tables.sql in your local.
2. Run the following curls. The curl works without any session id
as we have commented the session handling part for local testing. 
3. Login to the azkaban and get the session.id from the cookies section
   It will be against the key "azkaban.browser.session.id" and pass it in the below calls.
Get the space:
curl -X GET "http://localhost:8081/spaces/1?session.id=?"

Get all spaces:
curl -X GET "http://localhost:8081/spaces?session.id="

create space:
Place the body of the POST request in a file. In the below
example its body.json and then fire the below call. 

curl -X POST -H "Content-Type: application/json" --data @body.json 
"http://localhost:8081/spaces?session.id=?"

Sample response body:

{
  "name": "sample-07",
  "description": "sample 6",
  "admins": [
    "sarumuga"
  ],
  "watchers": [
    "gsalia"
  ]
}

Get executions filtered by Project Id and User.
curl -X GET "http://localhost:8081/executions?flow_id=basic_flow&project_id=1&session.id=<>"

Sample response body:
[ {
  "executionId" : 1,
  "submitUser" : "azkaban",
  "submitTime" : 1580954330693,
  "experimentId" : "none",
  "concurrentOption" : "skip",
  "failureAction" : "FINISH_CURRENTLY_RUNNING",
  "notifyOnFirstFailure" : false,
  "notifyFailureOnExecutionComplete" : false
} ]


Get job execution details.
curl -X GET "http://localhost:8081/executions/21024226/jobs/123"

Sample response body:
{
  "executionId": "21024226",
  "startTime": 1580855926989,
  "endTime": 1580856047132,
  "status": "FAILED",
  "attempts": [
    {
      "startTime": 1580855926989,
      "endTime": 1580855957019,
      "id": 0,
      "status": "FAILED"
    },
    {
      "startTime": 1580855957032,
      "endTime": 1580855987060,
      "id": 1,
      "status": "FAILED"
    },
    {
      "startTime": 1580855987070,
      "endTime": 1580856017098,
      "id": 2,
      "status": "FAILED"
    },
    {
      "startTime": 1580856017107,
      "endTime": 1580856047132,
      "id": 3,
      "status": "FAILED"
    }
  ]
}


Create a flow execution.
curl -X POST 'http://localhost:8081/executions' \
-H 'Content-Type: application/json' \
--data-raw '{
	"flowId": "2222",
	"flowVersion": 4,
	"experimentId": "70",
	"description": "A test execution.",
	"failureAction": "finishCurrent",
	"notifyOnFirstFailure": "true",
	"notifyFailureOnExecutionComplete": "true",
	"concurrentOption": "skip",
	"properties": {
		"root": {
			"retries": 3,
			"failure.emails": ["a@a.com", "b@b.com"],
			"success.emails": ["c@c.com"]
		}
	}
}'

Response body is empty


** Get all the flows for a project
curl -X GET http://localhost:8081/flows
[ {
  "id" : "1",
  "name" : "flow1_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "2",
    "projectVersion" : "1",
    "flowName" : "flow1_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow1_proj2",
    "flowId" : "1",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
}, {
  "id" : "2",
  "name" : "flow2_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "2",
    "projectVersion" : "1",
    "flowName" : "flow2_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow2_proj2",
    "flowId" : "2",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
}, {
  "id" : "3",
  "name" : "flow3_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "2",
    "projectVersion" : "1",
    "flowName" : "flow3_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow3_proj2",
    "flowId" : "3",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
} ]


** Get all the flows for a project, filter by project name
curl http://localhost:8081/flows?project_name=test-project1
[ {
  "id" : "1",
  "name" : "flow1_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "5",
    "projectVersion" : "2",
    "flowName" : "flow1_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow1_proj2",
    "flowId" : "1",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
}, {
  "id" : "2",
  "name" : "flow2_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "5",
    "projectVersion" : "2",
    "flowName" : "flow2_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow2_proj2",
    "flowId" : "2",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
}, {
  "id" : "3",
  "name" : "flow3_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "5",
    "projectVersion" : "2",
    "flowName" : "flow3_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow3_proj2",
    "flowId" : "3",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
} ]


** Get details for a flowId
curl  http://localhost:8081/flows/1
{
  "id" : "1",
  "name" : "flow1_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "2",
    "projectVersion" : "1",
    "flowName" : "flow1_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow1_proj2",
    "flowId" : "1",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : null,
  "modifiedByUser" : "gsalia"
}

** Get details for a flowId along with all the flow versions
curl  http://localhost:8081/flows/1?expand=versions
{
  "id" : "1",
  "name" : "flow1_proj5",
  "flowVersionCount" : 2,
  "projectId" : "5",
  "projectVersion" : "2",
  "admins" : null,
  "createdByUser" : "gsalia",
  "createdOn" : 1582577888795,
  "modifiedOn" : 1582577888800,
  "lastVersion" : {
    "projectId" : "5",
    "projectVersion" : "2",
    "flowName" : "flow1_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow1_proj2",
    "flowId" : "1",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  },
  "flowVersions" : [ {
    "projectId" : "5",
    "projectVersion" : "1",
    "flowName" : "flow1_proj2",
    "flowVersion" : "1",
    "createTime" : 1582577888795,
    "flowFileLocation" : "/tmp/flow1_proj2",
    "flowId" : "1",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  }, {
    "projectId" : "5",
    "projectVersion" : "2",
    "flowName" : "flow1_proj2",
    "flowVersion" : "2",
    "createTime" : 1582577888800,
    "flowFileLocation" : "/tmp2/flow1_proj2",
    "flowId" : "1",
    "experimental" : false,
    "dslVersion" : 2.0,
    "createdBy" : "gsalia",
    "locked" : false
  } ],
  "modifiedByUser" : "gsalia"
}
