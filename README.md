Azkaban is simple batch scheduler for constructing and running Hadoop jobs or other offline processes.

# What is that?

A batch job scheduler can be seen as a combination of the unix utilities cron and make. Batch jobs need to be scheduled to run periodically. They also typically have intricate dependencies chains—for example dependencies on various data extraction processes or previous steps. Larger processes may have 50 or 60 steps, some of which may run in parallel and others of which must wait for one another. Combining all these processes into a single program will allow you to control the dependency management but lead to sprawling monolithic programs which are difficult to test or maintain. Simply scheduling the individual pieces to run at different times avoids the monolithic problem, but introduces many timing assumptions that are inevitably broken. [Azkaban](http://sna-projects.com/azkaban "Azkaban")
 is a "workflow" scheduler that allows the pieces to be declaratively assembled into a single workflow and for that workflow to be scheduled to be run periodically.

A good batch workflow system allows a program to be built out of small reusable pieces that need not know about one another. By declaring dependencies you can control sequencing. Other functionality available from [Azkaban](http://sna-projects.com/azkaban "Azkaban") can then be layered on top of the job--email notifications of success or failure, resource locking, retry on failure, log collection, historical job runtime information, and so on.

# Why was it made?

Schedulers are readily available (both open source and commercial), but tend to be extremely unfriendly to work with--they are basically bad GUI's grafted onto 20 year old command line clients. We wanted something that made it reasonably easy to visualize job hierarchies and run times without the pain. Previous experience made it clear that a good batch programming framework can make batch programming easy and successful in the same way that a web framework can aid web development beyond what you can do with an HTTP library and sockets.
State of the project

We have been using [Azkaban](http://sna-projects.com/azkaban "Azkaban") internally at [LinkedIn](http://www.linkedin.com "LinkedIn") for the last nine months or so, and have about a hundred jobs running in it, mostly hadoop jobs or ETL of some sort. [Azkaban](http://sna-projects.com/azkaban "Azkaban") is quite new as an open source project, though, and we are working now to generalize it enough to make it useful for everyone.

Any patches, bug reports, or feature ideas are quite welcome. We have created a mailing list for this purpose.
