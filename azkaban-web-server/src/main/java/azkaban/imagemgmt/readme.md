1. Create Image Type metadata API

curl -X POST -H "Content-Type: application/json" --data @payload.json http://localhost:8081
/imageTypes?session.id=?

Payload: 
{
  "imageType": "kafka_push_job",
  "description": "kafka push job",
  "deployable": "IMAGE",
  "ownerships":[{"owner": "anath1", "role": "admin"},
                {"owner": "apruthi", "role": "admin"}]
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

http://localhost:8081/imageVersions?session.id=[]&imageType=spark_job&imageVersion=1.6.0

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
      "versionState":"new"
   }
]
