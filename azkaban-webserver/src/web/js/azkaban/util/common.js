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

function addClass(el, name) {
  if (!hasClass(el, name)) {
    var classes = el.getAttribute("class");
    classes += classes ? ' ' + name : '' +name;
    el.setAttribute("class", classes);
  }
}

function removeClass(el, name) {
  if (hasClass(el, name)) {
    var classes = el.getAttribute("class");
    el.setAttribute("class", classes.replace(new RegExp('(\\s|^)'+name+'(\\s|$)'),' ').replace(/^\s+|\s+$/g, ''));
  }
}

function hasClass(el, name) {
  var classes = el.getAttribute("class");
  if (classes == null) {
    return false;
  }
  return new RegExp('(\\s|^)'+name+'(\\s|$)').test(classes);
}

function sizeStrToBytes(str) {
  if (str.length == 0) {
    return 0;
  }
  var unit = str.charAt(str.length - 1)
  if (!isNaN(unit)) {
    return parseInt(str);
  }
  var val = parseInt(str.substring(0, str.length - 1));
  unit = unit.toUpperCase();
  if (unit == 'M') {
    val *= 0x100000;
  }
  else if (unit == 'G') {
    val *= 0x40000000;
  }
  return val;
}
