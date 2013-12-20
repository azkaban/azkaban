        <div class="col-lg-12">
          <table class="table table-bordered table-condensed table-striped">
            <tbody>
              <tr>
                <td class="flow-summary-key">Flow name</td>
                <td class="flow-summary-value">{flowName}</td>
                <td class="flow-summary-key">Project name</td>
                <td class="flow-summary-value">{projectName}</td>
              </tr>
              <tr>
                <td class="flow-summary-key">Run As</td>
                <td class="flow-summary-value">{user}</td>
                <td class="flow-summary-key">Job Types Used</td>
                <td class="flow-summary-value">{#jobTypes}{.} {/jobTypes}</td>
              </tr>
            </tbody>
          </table>
					
					<div class="panel panel-default">
						<div class="panel-heading">
							Scheduling
							{?schedule}
							<div class="pull-right">
								<button type="button" id="removeSchedBtn" class="btn btn-xs btn-danger" onclick="removeSched({schedule.scheduleId})" >Remove Schedule</button>
							</div>
							{/schedule}
						</div>
						{?schedule}
						<table class="table table-condensed table-bordered table-striped">
							<tbody>
								<tr>
									<td class="flow-summary-key">Schedule ID</td>
									<td class="flow-summary-value">{schedule.scheduleId}</td>
									<td class="flow-summary-key">Submitted By</td>
									<td class="flow-summary-value">{schedule.submitUser}</td>
								</tr>
								<tr>
									<td class="flow-summary-key">First Scheduled to Run</td>
									<td class="flow-summary-value">{schedule.firstSchedTime}</td>
									<td class="flow-summary-key">Repeats Every</td>
									<td class="flow-summary-value">{schedule.period}</td>
								</tr>
								<tr>
									<td class="flow-summary-key">Next Execution Time</td>
									<td class="flow-summary-value">{schedule.nextExecTime}</td>
									<td class="flow-summary-key">SLA</td>
									<td class="flow-summary-value">
									{?schedule.slaOptions}
										true 
									{:else} 
										false 
									{/schedule.slaOptions}
										<div class="pull-right">
											<button type="button" id="addSlaBtn" class="btn btn-xs btn-primary" onclick="slaView.initFromSched({schedule.scheduleId}, '{flowName}')" >Set SLA</button>
										</div>
									</td>
								</tr>
							</tbody>
						</table>
						{:else}
						<div class="panel-body">
							<div class="alert alert-info">
								<h4>No Schedule</h4>
								<p>This flow has not been scheduled.</p>
							</div>
						</div>
						{/schedule}
					</div>

          <div class="panel panel-default">
            <div class="panel-heading">Last Run Stats</div>
            {?lastRun}
            <table class="table table-bordered table-condensed table-striped">
              <tbody>
								<tr>
									<td class="flow-summary-key">Max Map Slots from Largest Job</td>
									<td>{lastRun.maxMapSlots}</td>
                </tr>
                <tr>
									<td class="flow-summary-key">Max Reduce Slots from Largest Job</td>
									<td>{lastRun.maxReduceSlots}</td>
								</tr>
								<tr>
									<td class="flow-summary-key">Total Map Slots from All Jobs</td>
									<td>{lastRun.totalMapSlots}</td>
                </tr>
                <tr>
									<td class="flow-summary-key">Total Reduce Slots from All Jobs</td>
									<td>{lastRun.totalReduceSlots}</td>
								</tr>
								<tr>
									<td class="flow-summary-key">Total Number of Jobs</td>
									<td>{lastRun.numJobs}</td>
                </tr>
                <tr>
									<td class="flow-summary-key">Longest Task Time</td>
									<td>{lastRun.longestTaskTime}</td>
								</tr>
              </tbody>
            </table>
            {:else}
            <div class="panel-body">
              <div class="alert alert-info">
                <h4>No last run stats available</h4>
                <p>Last run stats requires at least one successful run of the flow.</p>
              </div>
            </div>
            {/lastRun}
          </div>
        </div><!-- /.col-lg-12 -->


