        <div class="col-lg-12">
          <table class="table table-striped table-bordered table-condensed table-hover">
            <tbody>
              <tr>
                <td class="flow-summary-key">Workflow name</td>
                <td class="flow-summary-value">{general.flowId}</td>
                <td class="flow-summary-key">Project name</td>
                <td class="flow-summary-value">{general.projectName}</td>
              </tr>
              <tr>
                <td class="flow-summary-key">Workflow Hadoop User Name</td>
                <td class="flow-summary-value">{general.user}</td>
                <td class="flow-summary-key">Job Types Used</td>
                <td class="flow-summary-value">{general.apis}</td>
              </tr>
            </tbody>
          </table>

          <div class="panel panel-default">
            <div class="panel-heading">Last Run Stats</div>
            {?lastRun}
            <table class="table table-striped table-bordered table-condensed table-hover">
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
