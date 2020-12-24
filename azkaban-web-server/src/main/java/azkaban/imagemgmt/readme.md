1. Create Image Type metadata API

curl -X POST -H "Content-Type: application/json" --data @payload.json http://localhost:8081
/imageTypes?session.id=?

Payload: 
{
  "imageType": "kafka_push_job",
  "description": "kafka push job",
  "deployable": "IMAGE",
  "ownerships":[{"owner": "anath1", "role": "ADMIN"},
                {"owner": "apruthi", "role": "ADMIN"}]
}

Sample Response
Header: Location -> /imageTypes/{id}

2. Create image version metadata API

curl -X POST -H "Content-Type: application/json" --data @payload.json http://localhost:8081
/imageVersions?session.id=?

Payload:
{
    "imagePath": "path_spark_job",
    "imageVersion": "1.2.0",
    "imageType": "spark_job",
    "description": "spark job",
    "versionState": "NEW",
    "releaseTag": "1.5.5"
}

Sample Response
Header: Location -> /imageVersions/{id}

3. Get image versions metadata API

curl -X GET http://localhost:8081/imageVersions?session.id=[]&imageType=spark_job&imageVersion=1
.6.0

Sample Response Body:
[
   {
      "createdBy":"azkaban",
      "createdOn":"2020-11-11 09:11:27",
      "modifiedBy":null,
      "modifiedOn":null,
      "id":2,
      "description":"spark job",
      "releaseTag":"1.4.8",
      "imagePath":"path_spark_job",
      "imageVersion":"1.6.0",
      "imageType":"spark_job",
      "versionState":"NEW"
   }
]

4. Create image rampup API

curl -X POST -H "Content-Type: application/json" --data @payload.json http://localhost:8081
/imageRampup?session.id=?

Payload:
{
  "planName": "Rampup plan for hadoopJava",
  "imageType": "hadoopJava",
  "description": "Ramp up for hadoopJava",
  "activatePlan": true,
  "imageRampups":[{"imageVersion": "3.1.4", "rampupPercentage": "60"},
                {"imageVersion": "3.1.2", "rampupPercentage": "30"},
                {"imageVersion": "3.1.1", "rampupPercentage": "10"}]
}

Sample Response
Status: 201
Header: Location -> /imageRampup/{id}

5. Get image rampup API

curl -X GET http://localhost:8081/imageRampup?session.id=[]&imageType=hadoopJava

Sample Response Body:

{
    "id": 5,
    "createdBy": "anath1",
    "createdOn": "2020-12-23 09:34:40",
    "modifiedBy": "anath1",
    "modifiedOn": "2020-12-23 09:51:07",
    "planName": "Rampup plan for hadoop",
    "imageTypeName": "hadoopJava",
    "description": "Ramp up for hadoop",
    "active": true,
    "imageRampups": [
        {
            "id": 7,
            "createdBy": "anath1",
            "createdOn": "2020-12-23 09:34:40",
            "modifiedBy": "anath1",
            "modifiedOn": "2020-12-23 09:51:07",
            "planId": 5,
            "imageVersion": "3.1.1",
            "rampupPercentage": 10,
            "stabilityTag": "EXPERIMENTAL"
        },
        {
            "id": 6,
            "createdBy": "anath1",
            "createdOn": "2020-12-23 09:34:40",
            "modifiedBy": "anath1",
            "modifiedOn": "2020-12-23 09:51:07",
            "planId": 5,
            "imageVersion": "3.1.2",
            "rampupPercentage": 30,
            "stabilityTag": "EXPERIMENTAL"
        },
        {
            "id": 5,
            "createdBy": "anath1",
            "createdOn": "2020-12-23 09:34:40",
            "modifiedBy": "anath1",
            "modifiedOn": "2020-12-23 09:51:07",
            "planId": 5,
            "imageVersion": "3.1.4",
            "rampupPercentage": 60,
            "stabilityTag": "EXPERIMENTAL"
        }
    ]
}

6. Update image rampup API

curl -X POST -H "Content-Type: application/json" --data @payload.json http://localhost:8081
/imageRampup/hadoopJava?session.id=?

Payload:

{
  "planName": "Rampup plan for hadoop",
  "imageType": "hadoopjava",
  "description": "Ramp up for hadoop",
  "activatePlan": true,
  "imageRampups":[{"imageVersion": "3.1.4", "rampupPercentage": "80", "stabilityTag": "STABLE"},
                {"imageVersion": "3.1.2", "rampupPercentage": "10", "stabilityTag": "STABLE"},
                {"imageVersion": "3.1.1", "rampupPercentage": "10", "stabilityTag": "STABLE"}]
}

Response status: 200

7. Update image version API

curl -X POST -H "Content-Type: application/json" --data @payload.json http://localhost:8081
/imageVersions/5?session.id=?

Payload:
{
    "description": "Update to active version",
    "imageType": "command",
    "versionState": "ACTIVE"
}

Response status: 200