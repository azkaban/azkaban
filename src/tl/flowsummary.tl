					<h4>General</h4>
					<table>
						<tbody>
							<tr>
								<td>Workflow name</td>
								<td>{general.flowName}</td>
							</tr>
							<tr>
								<td>Workflow Purpose/Description</td>
								<td>{general.flowDescription}</td>
							</tr>
							<tr>
								<td>Project name</td>
								<td>{general.project}</td>
							</tr>
							<tr>
								<td>Name of scheduled item</td>
								<td>{general.flowId}</td>
							</tr>
							<tr>
								<td>Workflow Hadoop User Name</td>
								<td>{general.user}</td>
							</tr>
							<tr>
								<td>Responsible team</td>
								<td>{general.team}</td>
							</tr>
							<tr>
								<td>Responsible manager</td>
								<td>{general.manager}</td>
							</tr>
							<tr>
								<td>Responsible Developer</td>
								<td>{general.developer}</td>
							</tr>
							<tr>
								<td>Hadoop APIs Used</td>
								<td>{general.apis}</td>
							</tr>
							<tr>
								<td>Additional Hadoop APIs Used</td>
								<td>{general.additionalApis}</td>
							</tr>
							<tr>
								<td>Streaming Languages Used</td>
								<td>{general.streamingLanguages}</td>
							</tr>
							<tr>
								<td>Code Repository</td>
								<td>{general.codeRepository}</td>
							</tr>
							<tr>
								<td>Does this workflow perform any second, third, or higher level connection graph calculations?</td>
								<td>{general.higherLevelGraphOperations}</td>
							</tr>
							<tr>
								<td>Can this workflow be run with an empty <strong>/jobs</strong> directory</td>
								<td>{general.emptyJobsDirectory}</td>
							</tr>
							<tr>
								<td>If not, why not?</td>
								<td>{general.emptyJobsDirectoryReason}</td>
							</tr>
							<tr>
								<td>How does the result get to front-end servers?</td>
								<td>{general.toFrontEnd}</td>
							</tr>
						</tbody>
					</table>

					<h4>Scheduling</h4>
					<table>
						<tbody>
							<tr>
								<td>Max Map Slots from Largest Job</td>
								<td>{scheduling.maxMapSlots}</td>
							</tr>
							<tr>
								<td>Max Reduce Slots from Largest Job</td>
								<td>{scheduling.maxReduceSlots}</td>
							</tr>
							<tr>
								<td>Total Reduce Slots from All Jobs</td>
								<td>{scheduling.totalReduceSlots}</td>
							</tr>
							<tr>
								<td>Total Number of Jobs</td>
								<td>{scheduling.numJobs}</td>
							</tr>
							<tr>
								<td>Longest Task Time</td>
								<td>{scheduling.longestTaskTime}</td>
							</tr>
							<tr>
								<td>Required Schedule</td>
								<td>{scheduling.schedule}</td>
							</tr>
							<tr>
								<td>Launch Time</td>
								<td>{scheduling.launchTime}</td>
							</tr>
							<tr>
								<td>Total Workflow Run Time (hours)</td>
								<td>{scheduling.totalFlowTime}</td>
							</tr>
							<tr>
								<td>Expected Time of Completion</td>
								<td>{scheduling.expectedCompletionTime}</td>
							</tr>
							<tr>
								<td>Max Permitted Delay</td>
								<td>{scheduling.maxPermittedDelay}</td>
							</tr>
						</tbody>
					</table>

					<h4>Resources</h4>
					<table>
						<tbody>
							<tr>
								<td>Size of largest <code>-Xmx</code> value?</td>
								<td>{resources.largestXmx.size}</td>
								<td>If this is above 1G, please explain why</td>
								<td>{resources.largestXmx.reason}</td>
							</tr>
							<tr>
								<td>Do any jobs use <code>-Xms</code>?</td>
								<td>{resources.xms.using}</td>
								<td>If so, why?</td>
								<td>{resources.xms.reason}</td>
							</tr>
							<tr>
								<td>Is <em>intermediate compression</em> specifically turned on?</td>
								<td>{resources.intermediateCompression.on}</td>
								<td>If so, which codec</td>
								<td>{resources.intermediateCompression.codec}</td>
							</tr>
							<tr>
								<td>Are there combiners in use?</td>
								<td colspan="3">{resources.combiners}</td>
							</tr>
							<tr>
								<td>Size of largest <code>mapred.job.map.memory.mb</code></td>
								<td>{resources.largestMapredJobMapMemoryMb.size}</td>
								<td>Used by job</td>
								<td>{resources.largestMapredJobMapMemoryMb.job}</td>
							</tr>
							<tr>
								<td>Size of largest <code>mapred.job.reduce.memory.mb</code></td>
								<td>{resources.largestMapredJobReduceMemoryMb.size}</td>
								<td>Used by job</td>
								<td>{resources.largestMapredJobMapMemoryMb.job}</td>
							</tr>
						</tbody>
					</table>
