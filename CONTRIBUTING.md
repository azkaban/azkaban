# Contributing to Azkaban

:+1: First off, thanks for taking the time to contribute! :+1:

Please take a moment to read this document if you would like to contribute. Please feel free to 
reach out on [the Azkaban Google Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev) 
if you have any questions.

## Reporting issues
We [use Github issues](https://github.com/azkaban/azkaban/issues) to track bug reports, feature requests,
 and submitting pull requests.

If you find a bug:

Use the GitHub issue search to check whether the bug has already been reported.
If the issue has been fixed, try to reproduce the issue using the latest master branch of the repository.
If the issue still reproduces or has not yet been reported, try to isolate the problem before opening an issue.

Please don't report an issue as a way to ask a question. Use the Google group instead.

## Pull requests
Before embarking on making significant changes, please open an issue and ask first so that you do not risk 
duplicating efforts or spending time working on something that may be out of scope.

Please see [the Github documentation](https://help.github.com/articles/about-pull-requests/) for 
help on how to create a pull request.

Please give your pull request a clear title and description and note which issue(s) your pull request fixes.

*Important*: By submitting a patch, you agree to allow the project owners to license your work 
under [the Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).


## Styleguides
Azkaban follows [Google code style](http://google.github.io/styleguide/). The template file, 
`az-intellij-style.xml`, can be found in the root directory. It's based on the 
[intellij-java-google-style.xml config file](https://github.com/google/styleguide/blob/75c289f1d33836d1ff4bd94e6c9033673e320b58/intellij-java-google-style.xml) from the google/styleguide project.

Follow [the Intellij's code style help](https://www.jetbrains.com/help/idea/2017.1/code-style.html) 
to import and set up the style. Make sure to activate the AzkabanStyle by
 "copy the IDE scheme to the current project, using the Copy to Project... command."

Install and enable the intellij's 
[save actions plugin](https://github.com/dubreuia/intellij-plugin-save-actions) 
to reformat/refactor code automatically:

Please turn on all the options except 
  * Remove unused suppress warning annotation

It allows us to do following when saving a file:
* Organize imports
* Reformat code based on the code style
* Rearrange code (reorder methods, fields, etc.)
* Add final to local variable
* Add final to field
* Remove explicit generic type for diamond
* Qualify field access with this
* Remove final from private method
* Remove unnecessary semicolon
* Add missing @Override annotations

