.. _image-mgmt-api:

Image Management API
====================

**Authors**:
`Abhishek Nath <https://github.com/orgs/azkaban/people/abhisheknath2011>`_ ,
`Arvind Pruthi <https://github.com/orgs/azkaban/people/arvindpruthi>`_

**Contributors:**
`Janki Akhani <https://github.com/orgs/azkaban/people/jakhani>`_ ,
`Shardool <https://github.com/orgs/azkaban/people/sshardool>`_ ,
`Deepak Jaiswal <https://github.com/orgs/azkaban/people/djaiswal83>`_ ,
`Aditya Sharma <https://github.com/orgs/azkaban/people/aditya1105>`_

Ref: :file:`docs/containerization-design.rst`

.. contents:: Table of Contents
  :local:
  :depth: 2

Introduction
------------
In containerized Azkaban, the environment for a Pod that will orchestrate flows will be built dynamically at the time
of launching the flow. Azkaban core, jobtype and platform binaries, each will have their own, independent evolution
cycles with their own, separate docker “images”. Azkaban will have the ability to put together the required binaries
by layering the required images with their desired versions at run-time.

Key Requirements
----------------
1. The image created based on azkaban binaries, platform binaries etc. as part of the release process must be available
   for Azkaban to consume and use.
#. Image owners/developers should be able to roll out their images independently without having dependency on
   Azkaban codebase.
#. Image roll outs to be independent of Azkaban release.
#. Maintain a single source of truth for images and their associated metadata.
#. REST API for image rollouts and rampups to allow for a self-service model.

REST API EndPoints
------------------
Azkaban web server will host these APIs for each Azkaban cluster. Initially to start with there will be image management CRUD APIs and percentage wise image ramp up API.
API for populating new image type
This API will be used to populate newly created image types. As a result of invocation of this API, a new image type will be inserted in the image_types table. One owner and role (admin by default) can be specified during image creation. Separate ownerships API will be provided later to manage image ownerships. Ownership information will be stored in the image_ownerships table.

/imageTypes
***********

.. _create-image-type:

Register new image type (Create)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Method:** POST
- **Request URL:** /imageTypes
- **Request Body:**

+-----------------+-------------+--------------------------------------+
|   Field Name    |     Type    |            Description               |
+=================+=============+======================================+
|  imageType      |   String    |  Image Type Name (PK)                |
+-----------------+-------------+--------------------------------------+
|  description    |   String    |  Description of the image type       |
+-----------------+-------------+--------------------------------------+
|  deployable     |   enum      |  IMAGE (docker image), JAR or TAR    |
+-----------------+-------------+--------------------------------------+
|  ownerships     | JSON String | List of:                             |
|                 |             | {"owner": <user-id>, "role": <role>} |
|                 |             |                                      |
|                 |             | Possible roles: "admin" or "member"  |
+-----------------+-------------+--------------------------------------+

**Response:**

.. code-block::

  Status: 201 Created
  Header -> Location: /imageTypes/{id}

/imageVersions
**************
This endpoint is used to register or list image versions for a given image type. The API
assumes that image type is already registered (Create API for /imageTypes was called before) with Azkaban.
Also the image binary must already be uploaded to a location in artifactory, accessible to Azkaban.
Hence, by calling this API, the image admin is informing Azkaban for the availability
of a given image version.

.. _create-image-version:

Create (Register) new Image Version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Method:** POST
- **Request URL:** /imageVersions
- **Request Body:**

+-----------------+-------------+-------------------------------------------------+
|   Field Name    |     Type    |            Description                          |
+=================+=============+=================================================+
|  imagePath      |   String    |                                                 |
+-----------------+-------------+-------------------------------------------------+
|  imageVersion   |   String    |                                                 |
+-----------------+-------------+-------------------------------------------------+
|  description    |   String    |                                                 |
+-----------------+-------------+-------------------------------------------------+
|  versionState   |   String    | “new”, “active” “unstable” or “deprecated”      |
+-----------------+-------------+-------------------------------------------------+
|  releaseTag     |   String    |                                                 |
+-----------------+-------------+-------------------------------------------------+

**Response:**

.. code-block::

     Status: 201 Created
     Header -> Location: /imageVersions/{id}

.. _get-image-version:

Get Image Version Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^
This API gets the image metadata for an image type. It fetches all the active records for an image version.
If most_recent is specified, the api returns the latest active record. To get the metadata
for a specific image version, the Optional parameter; 'imageVersion" should be populated in
the request structure.
- **Method:** GET
- **Request URL:** /imageVersions?imageType=<image_type>
- **Request Body:**

+-----------------+-------------+-------------------------------------------------+
|   Field Name    |     Type    |            Description                          |
+=================+=============+=================================================+
| ``imageType``   | ``String``  |                                                 |
+-----------------+-------------+-------------------------------------------------+
| ``imageVersion``| ``String``  | OPTIONAL Parameter.                             |
+-----------------+-------------+-------------------------------------------------+

**Example:**

.. code-block::

     GET /imageVersions?imageType=spark_job&imageVersion=1.1.0

**Response:**

.. code-block::

     Status: 200 OK
     Returns the matching records
     Format: json

.. _update-image-version:

Update Image Version Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Update image version metadata such as state. Possible values for ``state`` are:
``new``, ``active``, ``unstable`` or “deprecated” etc. The state will be updated during the process of ramp up.

- **Method:** PATCH
- **Request URL:** /imageVersions/{imageType}
- **Request Body:**

+------------------+-------------+-----------------------------------------------------+
|    Field Name    |     Type    |            Description                              |
+==================+=============+=====================================================+
| ``versionState`` | ``String``  | ``new``, ``active``, ``unstable`` or ``deprecated`` |
+------------------+-------------+-----------------------------------------------------+

.. _delete-image-version:

Delete Image Metadata
^^^^^^^^^^^^^^^^^^^^^
Delete the image metadata for the given image version id. This API can be used to clean up stale metadata records.

- **Method:** DELETE
- **Request URL:** /imageVersions/{imageType}

/imageRampup
************

.. _create-rampup-plan:

Create new Rampup Plan
^^^^^^^^^^^^^^^^^^^^^^
- **Method:** POST
- **Request URL:** /imageRampup
- **Request Body:**

+-----------------+-------------+---------------------------------------------------------+
|   Field Name    |     Type    |            Description                                  |
+=================+=============+=========================================================+
| ``planName``    | ``String``  | User provided name                                      |
+-----------------+-------------+---------------------------------------------------------+
| ``imageType``   | ``String``  | Image type                                              |
+-----------------+-------------+---------------------------------------------------------+
| ``description`` | ``String``  | Description                                             |
+-----------------+-------------+---------------------------------------------------------+
| ``activePlan``  | ``boolean`` | ``True`` to mark the plan active, ``False`` otherwise.  |
+-----------------+-------------+---------------------------------------------------------+
| ``imageRampups``|  ``List``   | List of rampup definitions as shown below               |
+-----------------+-------------+---------------------------------------------------------+

**Rampup Definition json block:**

.. _rampup-definition:

.. code-block:: json

     {
      "imageVersion": "string",
      "rampupPercentage": "int",
      "stability_tag": "enum"   // [experimental/stable/unstable]
      }

Refer to `usage of this API <#use-case-image-rampup>`_ for implementing a Canary.

.. _get-rampup-plan:

Get an existing Image Rampup Plan
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Returns a list of matching rampup plans for a given imageType

- **Method:** GET
- **Request URL:** /imageRampup
- **Request Body:**

+-----------------+-------------+---------------------------------------------------------+
|   Field Name    |     Type    |            Description                                  |
+=================+=============+=========================================================+
| ``imageType``   | ``String``  | Image type                                              |
+-----------------+-------------+---------------------------------------------------------+

**Example:**

.. code-block::

     GET /imageRampup?imageType=spark_job

.. _update-rampup-plan:

Update an existing Image Rampup Plan
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Update the active rampup plan and rampup details for an image type.

- **Method:** PATCH
- **Request URL:** /imageRampup/{planName}
- **Request Body:**

+-----------------+-------------+---------------------------------------------------------+
|   Field Name    |     Type    |            Description                                  |
+=================+=============+=========================================================+
| ``activePlan``  | ``boolean`` | ``True`` to mark the plan active, ``False`` otherwise.  |
+-----------------+-------------+---------------------------------------------------------+
| ``imageRampups``|  ``List``   | List of `rampup definitions <#rampup-definition>`_      |
+-----------------+-------------+---------------------------------------------------------+

Use-Cases and Workflows
-----------------------

New Jobtype/Platform Binary
***************************
When a jobtype/platform developer wants to publish their binary for the 1st time, they need to
register their **imageType** with Azkaban clusters. Hence, the API to `create image type <#create-image-type>`_
should be called with the required details.

Following this, a `version of the image type must be published <#create-image-version>`_ to be used within Azkaban.
This is an Admin API and the privileges for this API are with the Azkaban team.

Image owner wants to publish a new version
******************************************
When a jobtype/platform developer wants to deploy a new version of their binary, they need to invoke the API
to `create new image version <#create-image-version>`_. All imageType users with role as: "admin" as well as
Azkaban admin will have privileges to invoke this API.

.. _use-case-image-rampup:

Image owner wants to create a new Ramp-up Plan
**********************************************
When an image owner wants to rampup a new version of their imageType, they can create a new Ramp-up plan for their
imageType. Here is the ramp-up process:

1. Only one ramp up plan can be created and activated at a time. The ramp up plan must have a new version (latest)
   and one or more previous versions. Ramp up percentage for all the versions in a plan must add up to 100.
#. If a new ramp up plan is created for an image type, the previous plan must be deactivated.
#. The image versions used in the ramp-up plan must be already published/registered with Azkaban and the corresponding
   images are expected to be available in Artifactory.
#. The ramp-up plan and ramp ups can be created using the `Create ramp-up plan API <#create-rampup-plan>`_.
#. The caller of the API must be registered with role as: ``admin`` for the imageType.
#. The ramp-up plan and ramp-up details for an image type can be updated using
   `Update ramp-up API <#update-rampup-plan>`_.
#. In the active ramp up plan, once the new version is successfully ramped up to 100%, the imageType owner must call
   `Update Image Version API <#update-image-version>`_ to mark the fully ramped-up image as: **active**. Also,
   past 100% ramp-up, it is advised for the image owner to de-activate the ramp-up plan using
   `Update ramp-up plan API <#update-rampup-plan>`_.

**Example:**

.. code-block::

  POST /image_rampup/sparkJob --body
    {
      "planName": "SparkJobRampupPlan",
      "description": "Ramping up the new release for spark job type",
      "activePlan": "True",
      "imageRampups": [
              {image_version:1.2.1, rampup_percentage: 70},
              {image_version:1.3, rampup_percentage: 30}
      ]
    }

Once this ramp-up plan is posted, Azkaban will automatically pick version: ``1.3``, 30% of the times and ``1.2.1``,
70% of the times.

Image owner wants to Ramp-up/Ramp-down an image version
*******************************************************
While ramp-up of an imageType is ongoing, it is expected that the image owner may want to increase/decrease the
percentage of specific image versions. For this purpose, the `Update Ramp-up plan API <#update-rampup-plan>`_
should be used to update the percentages for various versions that the image owner wants Azkaban to use.
The rules defined in `Create Ramp-up plan use case <#use-case-image-rammpup>`_ should still be followed

**Example:** The following API call will change the ramp-up plan so that Azkaban will pick version: ``1.3``, 40%
of the times and ``1.2.1``, 60% of the times.

.. code-block::

  PATCH /image_rampup/SparkJobRampupPlan --body
    {
      "imageRampups": [
              {image_version:1.2.1, rampup_percentage: 60},
              {image_version:1.3, rampup_percentage: 40}
      ]
    }


Authentication/Authorization
----------------------------
The existing mechanisms for Authentication/Authorization for Azkaban should extend to these APIs. Please note
that the API for `Create imageType <#create-image-type>`_ establishes the ownership for imageTypes. The
ownerships can be changed by Azkaban admin.

