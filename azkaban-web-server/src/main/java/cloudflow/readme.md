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

Sample body:

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
