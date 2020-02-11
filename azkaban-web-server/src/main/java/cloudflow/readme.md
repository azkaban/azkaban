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
  "notifyFailureFirst" : false,
  "notifyFailureLast" : false
} ]

Get execution response with details for a given execution.
curl -X GET "http://localhost:8081/executions/2?session.id=<>"

Sample response in file: cloudflow/docs/ExecutionDetailSample.md