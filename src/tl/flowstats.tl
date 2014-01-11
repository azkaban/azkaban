          <div class="panel panel-default">
						<div class="panel-heading">Resources</div>
            <table class="table table-striped table-bordered table-condensed">
              <tbody>
                <tr>
                  <td class="property-key">Max Map Slots</td>
                  <td class="property-value-half">{stats.maxMapSlots}</td>
                  <td class="property-key">Total Map Slots</td>
                  <td class="property-value-half">{stats.totalMapSlots}</td>
                </tr>
                <tr>
                  <td class="property-key">Max Reduce Slots</td>
                  <td class="property-value-half">{stats.maxReduceSlots}</td>
                  <td class="property-key">Total Reduce Slots</td>
                  <td class="property-value-half">{stats.totalReduceSlots}</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div class="panel panel-default">
						<div class="panel-heading">Parameters</div>
            <table class="table table-striped table-bordered table-condensed">
              <tbody>
                <tr>
                  <td class="property-key">Max -Xmx</td>
                  <td class="property-value-half">{stats.maxXmx}</td>
                  <td class="property-key">-Xms</td>
                  <td class="property-value-half">{stats.maxXms}</td>
                </tr>
                <tr>
                  <td class="property-key">Max <code>mapred.job.map.memory.mb</code></td>
                  <td class="property-value-half">{stats.maxJobMapMemoryMb}</td>
                  <td class="property-key">Max <code>mapred.job.reduce.memory.mb</code></td>
                  <td class="property-value-half">{stats.maxJobReduceMemoryMb}</td>
                </tr>
              </tbody>
            </table>
          </div>
