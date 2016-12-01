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
              <button type="button" id="removeSchedBtn" class="btn btn-sm btn-danger" onclick="removeSched({schedule.scheduleId})" >Remove Schedule</button>
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
                    <button type="button" id="addSlaBtn" class="btn btn-xs btn-primary" onclick="slaView.initFromSched({schedule.scheduleId}, '{flowName}')" >View/Set SLA</button>
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

          <h3>Last Run Stats</h3>
        </div>
      </div>
