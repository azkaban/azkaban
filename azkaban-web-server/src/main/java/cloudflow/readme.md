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

Get execution response with details for a given execution.
curl -X GET "http://localhost:8081/executions/2?session.id=<>"

Sample response in file: cloudflow/docs/ExecutionDetailSample.md

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
