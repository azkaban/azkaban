      <div class="row">
        <div class="col-xs-12">
          <h4>Resources</h4>
          <table class="table table-bordered table-condensed">
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
      </div>

      <div class="row">
        <div class="col-xs-12">
          <h4>Parameters</h4>
          <table class="table table-bordered table-condensed">
            <tbody>
              <tr>
                <td class="property-key">Max <code>-Xmx</code></td>
                <td class="property-value-half">{stats.xmx.str}</td>
                <td class="property-key">-Xms</td>
                <td class="property-value-half">
                  {?stats.xms.set}
                    {stats.xms.str}
                  {:else}
                    Not set.
                  {/stats.xms.set}
                </td>
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
      </div>
      <div class="row row-offcanvas row-offcanvas-left">
        <div class="col-xs-6 col-sm-3 sidebar-offcanvas">
          <ul class="nav nav-pills nav-stacked">
            <li class="active"><a href="#parameters-xmx" data-toggle="tab">Xmx</a></li>
            <li><a href="#parameters-xmx" data-toggle="tab">Xms</a></li>
            <li><a href="#parameters-map-memory" data-toggle="tab">mapred.job.map.memory.mb</a></li>
            <li><a href="#parameters-reduce-memory" data-toggle="tab">mapred.job.reduce.memory.mb</a></li>
          </ul>
        </div>
        <div class="col-xs-12 col-sm-9">
          <div class="tab-content">
            <div class="tab-pane active" id="parameters-xmx">
              Xmx
            </div>
            <div class="tab-pane" id="parameters-xms">
              Xms
            </div>
            <div class="tab-pane" id="parameters-map-memory">
              mapred.job.map.memory.mb
            </div>
            <div class="tab-pane" id="parameters-reduce-memory">
              mapred.job.reduce.memory.mb
            </div>
          </div>
        </div>
      </div>

      <div class="row">
        <div class="col-xs-12">
          <h4>Counters</h4>
        </div>
      </div>

