        <div class="col-lg-12">
          <table class="table table-striped table-condensed table-bordered table-hover">
            <thead>
              <tr>
                <!--th class="execid">Execution Id</th-->
                <th>ID</th>
                <th>Submitted By</th>
                <th class="date">First Scheduled to Run</th>
                <th class="date">Next Execution Time</th>
                <th class="date">Repeats Every</th>
                <th>Has SLA</th>
                <th colspan="2" class="action ignoresort">Action</th>
              </tr>
            </thead>
            <tbody>
            {#schedules}
              <tr>
                <td>{scheduleId}</td>
                <td>{submitUser}</td>
                <td>{firstSchedTime}</td>
                <td>{nextExecTime}</td>
                <td>{period}</td>
                <td>{?slaOptions} true {:else} false {/slaOptions}</td>
                <td><button type="button" id="removeSchedBtn" class="btn btn-sm btn-danger" onclick="removeSched({scheduleId})" >Remove Schedule</button></td>
                <td><button type="button" id="addSlaBtn" class="btn btn-sm btn-primary" onclick="slaView.initFromSched({scheduleId}, '{flowName}')" >Set SLA</button></td>
              </tr>
            {:else}
              <tr>
                <td colspan="8">No scheduled flow found.</td>
              </tr>
            {/schedules}
            </tbody>
          </table>
        </div>
