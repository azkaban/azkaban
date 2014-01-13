      <div class="row">
        <div class="col-xs-12">
          <h4>Resources</h4>
          <table class="table table-bordered table-condensed table-striped">
            <thead>
              <tr>
                <th class="property-key">Resource</th>
                <th class="property-key">Value</th>
                <th>Job Name</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td class="property-key">Max Map Slots</td>
                <td>{stats.mapSlots.max}</td>
                <td>{stats.mapSlots.job}</td>
              </tr>
              <tr>
                <td class="property-key">Total Map Slots</td>
                <td colspan="2">{stats.totalMapSlots}</td>
              </tr>
              <tr>
                <td class="property-key">Max Reduce Slots</td>
                <td>{stats.reduceSlots.max}</td>
                <td>{stats.reduceSlots.job}</td>
              </tr>
              <tr>
                <td class="property-key">Total Reduce Slots</td>
                <td colspan="2">{stats.totalReduceSlots}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="row">
        <div class="col-xs-12">
          <h4>Parameters</h4>
          <table class="table table-bordered table-condensed table-striped">
            <thead>
              <tr>
                <th class="property-key">Parameter</th>
                <th class="property-key">Value</th>
                <th>Job Name</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td class="property-key">Max <code>-Xmx</code></td>
                <td>{stats.xmx.str}</td>
                <td>{stats.xmx.job}</td>
              </tr>
              <tr>
                <td class="property-key">Max -Xms</td>
                {?stats.xms.set}
                <td>
                  {stats.xms.str}
                </td>
                <td>
                  {stats.xms.job}
                </td>
                {:else}
                <td colspan="2">
                  Not set.
                </td>
                {/stats.xms.set}
              </tr>
              <tr>
                <td class="property-key">Max <code>mapred.job.map.memory.mb</code></td>
                <td>{stats.jobMapMemoryMb.max}</td>
                <td>{stats.jobMapMemoryMb.job}</td>
              </tr>
              <tr>
                <td class="property-key">Max <code>mapred.job.reduce.memory.mb</code></td>
                <td>{stats.jobReduceMemoryMb.max}</td>
                <td>{stats.jobReduceMemoryMb.job}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="row">
        <div class="col-xs-12">
          <h4>Counters</h4>
          <table class="table table-bordered table-condensed">
            <tbody>
              <tr>
                <td class="property-key">Max <code>FILE_BYTES_READ</code></td>
                <td class="property-value-half">{stats.maxFileBytesRead}</td>
                <td class="property-key">Max <code>HDFS_BYTES_READ</code></td>
                <td class="property-value-half">{stats.maxHdfsBytesRead}</td>
              </tr>
              <tr>
                <td class="property-key">Max <code>FILE_BYTES_WRITTEN</code></td>
                <td class="property-value-half">{stats.}</td>
                <td class="property-key">Max <code>mapred.job.reduce.memory.mb</code></td>
                <td class="property-value-half">{stats.maxJobReduceMemoryMb}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
