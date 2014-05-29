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

azkaban.ContextMenuView = Backbone.View.extend({
  events: {
  },

  initialize: function(settings) {
    var div = this.el;
    $('body').click(function(e) {
      $(".contextMenu").remove();
    });
    $('body').bind("contextmenu", function(e) {$(".contextMenu").remove()});
  },

  show: function(evt, menu) {
    console.log("Show context menu");
    $(".contextMenu").remove();
    var x = evt.pageX;
    var y = evt.pageY;

    var contextMenu = this.setupMenu(menu);
    $(contextMenu).css({top: y, left: x});
    $(this.el).after(contextMenu);
  },

  hide: function(evt) {
    console.log("Hide context menu");
    $(".contextMenu").remove();
  },

  handleClick: function(evt) {
    console.log("handling click");
  },

  setupMenu: function(menu) {
    var contextMenu = document.createElement("div");
    $(contextMenu).addClass("contextMenu");
    var ul = document.createElement("ul");
    $(contextMenu).append(ul);

    for (var i = 0; i < menu.length; ++i) {
      var menuItem = document.createElement("li");
      if (menu[i].break) {
        $(menuItem).addClass("break");
        $(ul).append(menuItem);
        continue;
      }
      var title = menu[i].title;
      var callback = menu[i].callback;
      $(menuItem).addClass("menuitem");
      $(menuItem).text(title);
      menuItem.callback = callback;
      $(menuItem).click(function() {
        $(contextMenu).hide();
        this.callback.call();
      });

      if (menu[i].submenu) {
        var expandSymbol = document.createElement("div");
        $(expandSymbol).addClass("expandSymbol");
        $(menuItem).append(expandSymbol);

        var subMenu = this.setupMenu(menu[i].submenu);
        $(subMenu).addClass("subMenu");
        subMenu.parent = contextMenu;
        menuItem.subMenu = subMenu;
        $(subMenu).hide();
        $(this.el).after(subMenu);

        $(menuItem).mouseenter(function() {
          $(".subMenu").hide();
          var menuItem = this;
          menuItem.selected = true;
          setTimeout(function() {
            if (menuItem.selected) {
              var offset = $(menuItem).offset();
              var left = offset.left;
              var top = offset.top;
              var width = $(menuItem).width();
              var subMenu = menuItem.subMenu;

              var newLeft = left + width - 5;
              $(subMenu).css({left: newLeft, top: top});
              $(subMenu).show();
            }
          }, 500);
        });
        $(menuItem).mouseleave(function() {this.selected = false;});
      }
      $(ul).append(menuItem);
    }

    return contextMenu;
  }
});

var contextMenuView;
$(function() {
  contextMenuView = new azkaban.ContextMenuView({el:$('#contextMenu')});
  contextMenuView.hide();
});
