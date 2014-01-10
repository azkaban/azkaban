        <div class="col-xs-12">
          <table class="table table-bordered table-condensed">
            <tbody>
              <tr>
                <td class="property-key">Flow name</td>
                <td class="property-value-half">{flowName}</td>
                <td class="property-key">Project name</td>
                <td class="property-value-half">{projectName}</td>
              </tr>
              <tr>
                <td class="property-key">Run As</td>
                <td class="property-value-half">{user}</td>
                <td class="property-key">Job Types Used</td>
                <td class="property-value-half">{#jobTypes}{.} {/jobTypes}</td>
              </tr>
            </tbody>
          </table>
				
          <h3>
            Scheduling
            {?schedule}
            <div class="pull-right">
              <button type="button" id="removeSchedBtn" class="btn btn-xs btn-danger" onclick="removeSched({schedule.scheduleId})" >Remove Schedule</button>
            </div>
            {/schedule}
          </h3>
					<div class="panel panel-default">
						<div class="panel-heading">
							Scheduling
						</div>
						{?schedule}
						<table class="table table-condensed table-bordered">
							<tbody>
								<tr>
									<td class="property-key">Schedule ID</td>
									<td class="property-value-half">{schedule.scheduleId}</td>
									<td class="property-key">Submitted By</td>
									<td class="property-value-half">{schedule.submitUser}</td>
								</tr>
								<tr>
									<td class="property-key">First Scheduled to Run</td>
									<td class="property-value-half">{schedule.firstSchedTime}</td>
									<td class="property-key">Repeats Every</td>
									<td class="property-value-half">{schedule.period}</td>
								</tr>
								<tr>
									<td class="property-key">Next Execution Time</td>
									<td class="property-value-half">{schedule.nextExecTime}</td>
									<td class="property-key">SLA</td>
									<td class="property-value-half">
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
								<h4>None</h4>
								<p>This flow has not been scheduled.</p>
							</div>
						</div>
						{/schedule}
					</div>
        </div>

        <div class="col-xs-12" id="last-run-container">
          <div class="alert alert-info" id="analyze-last-run">
            <h4>Analyze last run</h4>
            <p>Analyze the last run for aggregate performance statistics.</p>
            <p><strong>Note:</strong> this may take a few minutes, especially if your flow is large.</p>
            <p>
              <button type="button" id="analyze-btn" class="btn btn-primary">Analyze</button>
            </p>
          </div>
        </div><!-- /.col-lg-12 -->


