<div class="row">
  <div class="col-xs-12">
    <table class="table table-bordered table-condensed">
      <tbody>
      <tr>
        <td class="property-key">Project name</td>
        <td>{projectName}</td>
      </tr>
      <tr>
        <td class="property-key">Job Types Used</td>
        <td>{#jobTypes}{.} {/jobTypes}</td>
      </tr>
      <tr>
        <td class="property-key">Condition</td>
        {?condition}
          <td>{condition}</td>
        {:else}
          <td>none</td>
        {/condition}
      </tr>
      </tbody>
    </table>
  </div>
</div>

<div class="row">
  <div class="col-xs-12">
    <h3>
      Scheduling
      {?schedule}
        <div class="pull-right">
          <button type="button" id="removeSchedBtn" class="btn btn-sm btn-danger"
                  onclick="removeSched({schedule.scheduleId})">Remove Schedule
          </button>
        </div>
      {/schedule}
    </h3>
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

          <td class="property-key">
            {?schedule.cronExpression}
              Cron Expression
            {:else}
              Repeats Every
            {/schedule.cronExpression}
          </td>

          <td class="property-value-half">
            {?schedule.cronExpression}
              {schedule.cronExpression}
            {:else}
              {schedule.period}
            {/schedule.cronExpression}
          </td>

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
              <button type="button" id="addSlaBtn" class="btn btn-xs btn-primary"
                      onclick="slaView.initFromSched({schedule.scheduleId}, '{flowName}')">View/Set
                SLA
              </button>
            </div>
          </td>
        </tr>
        </tbody>
      </table>
    {:else}
      <div class="callout callout-default">
        <h4>None</h4>
        <p>This flow has not been scheduled.</p>
      </div>
    {/schedule}

    <div class="row">
      <div class="col-xs-12">
        <h3>
          Flow Trigger
        </h3>
        {?flowtrigger}
          <table class="table table-condensed table-bordered">
            <tbody>
            <tr>
              <td class="property-key">Max Wait Mins</td>
              <td class="property-value-half">{flowtrigger.maxWaitMin}</td>
              <td class="property-key">Submitted By</td>
              <td class="property-value-half">{flowtrigger.submitUser}</td>
            </tr>
            <tr>
              <td class="property-key">First Scheduled to Run</td>
              <td class="property-value-half">{flowtrigger.firstSchedTime}</td>

              <td class="property-key">
                Cron Expression
              </td>

              <td class="property-value-half">
                {?flowtrigger.cronExpression}
                  {flowtrigger.cronExpression}
                {/flowtrigger.cronExpression}
              </td>

            </tr>
            <tr>
              <td class="property-key">Next Execution Time</td>
              <td class="property-value-half">{flowtrigger.nextExecTime}</td>
              <td class="property-key">Has Dependency</td>
              <td class="property-value-half">
                {?flowtrigger.dependencies}
                  true
                {:else}
                  false
                {/flowtrigger.dependencies}
                <div class="pull-right">
                  <button type="button" id="showFlowTrigger" data-toggle="modal"
                          class="btn btn-sm btn-info" data-target="#dependencyList">Show Dependency
                  </button>
                </div>

                <div class="modal fade" id="dependencyList" tabindex="-1"
                     role="dialog"
                     aria-labelledby="dependencyLabel">
                  <div class="modal-dialog" role="document">
                    <div class="modal-content">
                      <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                          <span aria-hidden="true">&times;</span></button>
                        <h4 class="modal-title" id="dependencyLabel">Dependencies</h4>
                      </div>
                      <div class="modal-body">
                        <pre>{flowtrigger.dependencies}</pre>
                      </div>
                      <div class="modal-footer">
                        <button type="button" class="btn btn-default" data-dismiss="modal">Close
                        </button>
                      </div>
                    </div>
                  </div>
                </div>

              </td>
            </tr>
            </tbody>
          </table>
        {:else}
          <div class="callout callout-default">
            <h4>None</h4>
            <p>This flow doesn't have flow trigger.</p>
          </div>
        {/flowtrigger}

      </div>

      <h3>Last Run Stats</h3>
    </div>
  </div>
