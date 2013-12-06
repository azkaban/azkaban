					<div class="panel panel-default">
						<div class="panel-heading">General</div>
						<table class="table table-striped table-bordered table-condensed table-hover">
							<tbody>
								<tr>
									<td class="worksheet-key">Workflow name</td>
									<td>{general.flowId}</td>
								</tr>
								<tr class="editRow">
									<td class="worksheet-key">Workflow Purpose/Description</td>
									<td class="editable"><span class="spanValue">{general.flowDescription}</span></td>
								</tr>
								<tr>
									<td class="worksheet-key">Project name</td>
									<td>{general.projectName}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Name of scheduled item</td>
									<td>{general.flowId}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Workflow Hadoop User Name</td>
									<td>{general.user}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Responsible team</td>
									<td class="editable"><span class="spanValue">{general.team}</span></td>
								</tr>
								<tr>
									<td class="worksheet-key">Responsible manager</td>
									<td class="editable"><span class="spanValue">{general.manager}</span></td>
								</tr>
								<tr>
									<td class="worksheet-key">Responsible Developer</td>
									<td class="editable"><span class="spanValue">{general.developer}</span></td>
								</tr>
								<tr>
									<td class="worksheet-key">Hadoop APIs Used</td>
									<td>{general.apis}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Additional Hadoop APIs Used</td>
									<td>{general.additionalApis}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Streaming Languages Used</td>
									<td>{general.streamingLanguages}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Code Repository</td>
									<td>{general.codeRepository}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Does this workflow perform any second, third, or higher level connection graph calculations?</td>
									<td>{general.higherLevelGraphOperations}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Can this workflow be run with an empty <strong>/jobs</strong> directory</td>
									<td>{general.emptyJobsDirectory}</td>
								</tr>
								<tr>
									<td class="worksheet-key">If not, why not?</td>
									<td>{general.emptyJobsDirectoryReason}</td>
								</tr>
								<tr>
									<td class="worksheet-key">How does the result get to front-end servers?</td>
									<td>{general.toFrontEnd}</td>
								</tr>
							</tbody>
						</table>
					</div>

					<div class="panel panel-default">
						<div class="panel-heading">Scheduling</div>
						<table class="table table-striped table-bordered table-condensed table-hover">
							<tbody>
								<tr>
									<td class="worksheet-key">Max Map Slots from Largest Job</td>
									<td>{scheduling.maxMapSlots}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Max Reduce Slots from Largest Job</td>
									<td>{scheduling.maxReduceSlots}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Total Reduce Slots from All Jobs</td>
									<td>{scheduling.totalReduceSlots}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Total Number of Jobs</td>
									<td>{scheduling.numJobs}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Longest Task Time</td>
									<td>{scheduling.longestTaskTime}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Required Schedule</td>
									<td>{scheduling.schedule}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Launch Time</td>
									<td>{scheduling.launchTime}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Total Workflow Run Time (hours)</td>
									<td>{scheduling.totalFlowTime}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Expected Time of Completion</td>
									<td>{scheduling.expectedCompletionTime}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Max Permitted Delay</td>
									<td>{scheduling.maxPermittedDelay}</td>
								</tr>
							</tbody>
						</table>
					</div>

					<div class="panel panel-default">
						<div class="panel-heading">Resources</div>
						<table class="table table-striped table-bordered table-condensed table-hover">
							<tbody>
								<tr>
									<td class="worksheet-key">Is any of the code specifically multi-threaded?</td>
									<td colspan="3">{resources.multithreaded}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Do you 'fat jar' any hadoop-core jars?</td>
									<td colspan="3">{resources.fatJar}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Which job has the largest spill count?</td>
									<td>{resources.largestSpill.job}</td>
									<td class="worksheet-key">Largest spill count for any given task?</td>
									<td>{resources.largestSpill.count}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Is there a distributed cache in use?</td>
									<td>{resources.distributedCache.using}</td>
									<td class="worksheet-key">How big is the distributed cache?</td>
									<td>{resources.distributedCache.size}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Size of largest <code>-Xmx</code> value?</td>
									<td>{resources.largestXmx.size}</td>
									<td class="worksheet-key">If this is above 1G, please explain why</td>
									<td>{resources.largestXmx.reason}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Do any jobs use <code>-Xms</code>?</td>
									<td>{resources.xms.using}</td>
									<td class="worksheet-key">If so, why?</td>
									<td>{resources.xms.reason}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Is <em>intermediate compression</em> specifically turned on?</td>
									<td>{resources.intermediateCompression.on}</td>
									<td class="worksheet-key">If so, which codec</td>
									<td>{resources.intermediateCompression.codec}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Are there combiners in use?</td>
									<td colspan="3">{resources.combiners}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Size of largest <code>mapred.job.map.memory.mb</code></td>
									<td>{resources.largestMapredJobMapMemoryMb.size}</td>
									<td class="worksheet-key">Used by job</td>
									<td>{resources.largestMapredJobMapMemoryMb.job}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Size of largest <code>mapred.job.reduce.memory.mb</code></td>
									<td>{resources.largestMapredJobReduceMemoryMb.size}</td>
									<td class="worksheet-key">Used by job</td>
									<td>{resources.largestMapredJobMapMemoryMb.job}</td>
								</tr>
							</tbody>
						</table>
					</div>

					<div class="panel panel-default">
						<div class="panel-heading">Input/Output</div>
						<table class="table table-striped table-bordered table-condensed table-hover">
							<tbody>
								<tr>
									<td class="worksheet-key">List of input HDFS file paths</td>
									<td>{io.hdfsPaths}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Number of files generated (hadoop dfs -count)</td>
									<td>{io.hdfsFileCount}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Average size of files generated in GB</td>
									<td>{io.averageFileSize}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Size of intermediate data (content of /jobs - output going to production) in GB on HDFS</td>
									<td>{io.intermediateFileSize}</td>
								</tr>
								<tr>
									<td class="worksheet-key">Size of final output data in GB on HDFS</td>
									<td>{io.finalOutputSize}</td>
								</tr>
							</tbody>
						</table>
					</div>
