.. Second Sphinx Proj documentation master file, created by
   sphinx-quickstart on Mon Jul 16 15:29:27 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Azkaban documentation!
==============================================

*****
What is Azkaban
*****
Azkaban is a distributed Workflow Manager, implemented at LinkedIn to solve the problem of Hadoop job dependencies. We had jobs that needed to run in order, from ETL jobs to data analytics products.

See the :ref:`GetStartedHead` for more details.

*****
Features
*****

- Distributed Multiple Executor
- MySQL Retry
- Friendly UI
- Conditional Workflow
- Data Triggers
- High Security
- Support plug-in extensions, from Web UI to job Execution
- Full Authorship management system


.. toctree::
   :maxdepth: 2
   :caption: Contents:


   getStarted
   configuration
   userManager
   createFlows
   useAzkaban
   eventTrigger
   ajaxApi
   howTo
   plugins
   jobTypes


*****
Community
*****
Users and development team are in the `Gitter <https://gitter.im/azkaban-workflow-engine/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge>`_.

Developers interested in getting more involved with Azkaban may join the mailing lists `mailing lists <https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev>`_, `report bugs <https://github.com/azkaban/azkaban/issues>`_, and make `contributions <https://github.com/azkaban/azkaban/pulls>`_.




.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
