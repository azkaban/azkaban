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

azkaban.TableSorter = Backbone.View.extend({
  events: {
    "click .sortable": "handleClickSort"
  },

  initialize: function(settings) {
    $(this.el).addClass("sortableTable");

    var thead = $(this.el).children("thead");
    var th = $(thead).find("th");

    $(th).addClass("sortable");
    $("th.ignoresort").removeClass("sortable");
    var sortDiv = document.createElement("div");

    $(sortDiv).addClass("sortIcon");

    $(th).append(sortDiv);

    var tbody = $(this.el).children("tbody");
    var rows = $(tbody).children("tr");

    var row;
    for (var i = 0; i < rows.length; ++i ) {
      var nextRow = rows[i];
      if (row && $(nextRow).hasClass("childrow")) {
        if (!row.childRows) {
          row.childRows = new Array();
        }
        row.childRows.push(nextRow);
      }
      else {
        row = nextRow;
      }
    }

    if (settings.initialSort) {
      this.toggleSort(settings.initialSort);
    }
  },

  handleClickSort: function(evt) {
    this.toggleSort(evt.currentTarget);
  },

  toggleSort: function(th) {
    console.log("sorting by index " + $(th).index());
    if ($(th).hasClass("asc")) {
      $(th).removeClass("asc");
      $(th).addClass("desc");
      // Sort to descending

      this.sort($(th).index(), true);
    }
    else if ($(th).hasClass("desc")) {
      $(th).removeClass("desc");
      $(th).addClass("asc");

      this.sort($(th).index(), false);
    }
    else {
      $(th).parent().children(".sortable").removeClass("asc").removeClass("desc");
      $(th).addClass("asc");

      this.sort($(th).index(), false);
    }
  },

  sort: function(index, desc) {
    var tbody = $(this.el).children("tbody");
    var rows = $(tbody).children("tr");

    var tdToSort = new Array();
    for (var i = 0; i < rows.length; ++i) {
      var row = rows[i];
      if (!$(row).hasClass("childrow")) {
        var td = row.children[index];
        tdToSort.push(td);
      }
    }

    if (desc) {
      tdToSort.sort(function(a,b) {
        var texta = $(a).text().trim().toLowerCase();
        var textb = $(b).text().trim().toLowerCase();

        if (texta < textb) {
          return 1;
        }
        else if (texta > textb) {
          return -1;
        }
        else {
          return 0;
        }
      });
    }
    else {
      tdToSort.sort(function(a,b) {
        var texta = $(a).text().trim().toLowerCase();
        var textb = $(b).text().trim().toLowerCase();

        if (texta < textb) {
          return -1;
        }
        else if (texta > textb) {
          return 1;
        }
        else {
          return 0;
        }
      });
    }

    var sortedTR = new Array();
    for (var i = 0; i < tdToSort.length; ++i) {
      var tr = $(tdToSort[i]).parent();
      sortedTR.push(tr);

      var childRows = tr[0].childRows;
      if (childRows) {
        for(var j=0; j < childRows.length; ++j) {
          sortedTR.push(childRows[j]);
        }
      }
    }

    for (var i = 0; i < sortedTR.length; ++i) {
      $(tbody).append(sortedTR[i]);
    }
  },

  render: function() {
    console.log("render sorted table");
  }
});
