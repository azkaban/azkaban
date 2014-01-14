/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

$.namespace('azkaban');

azkaban.TimeGraphView = Backbone.View.extend({
	events: {
	},
	
	initialize: function(settings) {
		this.model.bind('render', this.render, this);
		this.model.bind('change:page', this.render, this);
    this.modelField = settings.modelField;
    this.element = settings.el;
    this.render();
	},
	
	render: function(self) {
		var series = this.model.get(this.modelField);
    if (series == null) {
      return;
    }

    var data = [];
	  for (var i = 0; i < series.length; ++i) {
      if (series[i].startTime == null || series[i].endTime == null) {
        console.log("Each element in series must have startTime and endTime");
        return;
      }
      var startTime = series[i].startTime;
      var endTime = series[i].endTime;
      if (endTime == -1) {
        endTime = new Date().getTime();
      }

      data.push({ 
        time: endTime,
        duration: endTime - startTime
      });
    }

    Morris.Line({
      element: this.element,
      data: data,
      xkey: 'time',
      ykeys: ['duration'],
      labels: ['Duration']
    });
	}
});
