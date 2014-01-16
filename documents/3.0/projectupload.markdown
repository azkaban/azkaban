---
layout: documents
nav: projectupload
context: ../..
---

#Upload Projects

Click on the __Upload__ button. You will see the following dialog.

<img class="shadowimg" title="Upload" src="./images/uploadprojects.png" ALT="Upload" width="400" />

Select the archive file of your workflow files that you want to upload. Currently Azkaban only supports \*.zip files.
The zip should contain the \*.job files and any files needed to run your jobs. Job names must be unique
in a project. 

Azkaban will validate the contents of the zip to make sure that dependencies are met and that there's
no cyclical dependencies detected.  If it finds any invalid flows, the upload will fail.

Uploads overwrite all files in the project. Any changes made to jobs will be wiped out after a new zip file is
uploaded.

After a successful upload, you should see all of your flows listed on the screen.