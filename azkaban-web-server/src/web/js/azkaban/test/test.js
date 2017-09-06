function addClass(el, newClass) {
  if (el.className.indexOf(newClass) === -1) {
    el.className += newClass;
  }
}

var rewire = require('rewire'),
    dateJs = rewire('../util/date.js');

var moment = require('moment-timezone');

var chai = require('chai'),
    assert = chai.assert;

//Some Test instances
describe('addClass', function () {
  it('should add class into element', function () {
    var element = {className: ''};

    addClass(element, 'test-class');

    assert.equal(element.className, 'test-class');
  });

  it('should not add a class which already exists in element', function () {
    var element = {className: 'exists'};

    addClass(element, 'exists');

    var numClasses = element.className.split(' ').length;
    assert.equal(numClasses, 1);
  });

  // it('should append new class after existing one', function() {
  //    var element = { className: 'exists' };
  //    addClass(element, 'new-class');
  //    var classes = element.className.split(' ');
  //    assert.equal(classes[1], 'new-class');
  //  });
});

//Test the functionality from Unix Cron to Quartz
describe('CronTransformation', function () {

  var testStrFromCronToQuartz = dateJs.__get__('transformFromQuartzToUnixCron');

  it('should transfer correctly', function () {

    assert.equal(testStrFromCronToQuartz('0 3 * * 5'), '0 3 * * 4');
    assert.equal(testStrFromCronToQuartz('0 3 * * 5-7'), '0 3 * * 4-6');
    assert.equal(testStrFromCronToQuartz('0 3 * * 1,3-5 2016'),
        '0 3 * * 0,2-4 2016');
    assert.equal(testStrFromCronToQuartz('0 3 * * 5#3'), '0 3 * * 4#3');
    assert.equal(testStrFromCronToQuartz('0 3 * * 5-7#3'), '0 3 * * 4-6#3');
    assert.equal(testStrFromCronToQuartz('0 3 * * 1,3-5#3 2016'),
        '0 3 * * 0,2-4#3 2016');
  });
});

//Test the Validity of a Quartz Cron String
describe('ValidateQuartzStr', function () {

  var validateQuartzStr = dateJs.__get__('validateQuartzStr');

  it('validate Quartz String corretly', function () {

    assert.equal(validateQuartzStr('0 3 * * 5'), 'NUM_FIELDS_ERROR');
    assert.equal(validateQuartzStr('0 3 * *'), 'NUM_FIELDS_ERROR');
    assert.equal(validateQuartzStr('0 3 * * 5 23 3 2017'), 'NUM_FIELDS_ERROR');
    assert.equal(validateQuartzStr('0 3 * * 5 *'), 'DOW_DOM_STAR_ERROR');
    assert.equal(validateQuartzStr('0 3 * * 5 *'), 'DOW_DOM_STAR_ERROR');
    assert.equal(validateQuartzStr('0 3 * 5 5 * 2019'), 'DOW_DOM_STAR_ERROR');
    assert.equal(validateQuartzStr('0 3 * 5 5 ? 2018'), 'VALID');
    assert.equal(validateQuartzStr('0 3 * ? 5 FRI'), 'VALID');
    assert.equal(validateQuartzStr('0 3 * ? 5 3-6'), 'VALID');

  });
});

//Test moment js and moment timezone js
describe('momentJSTest', function () {

  var momentObj = moment();

  it('momentJSTest', function () {
    assert.equal(moment('20170411', 'YYYYMMDD').format('MM-YYYY'), '04-2017');
  });

  it('momentTimezoneTest', function () {

    // Refer to https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/UTC
    // month: 3 represents April
    var dateTime = Date.UTC(2017, 3, 11, 11, 17, 0);
    assert.equal(moment(dateTime).tz('America/Los_Angeles').format("LLL"),
        'April 11, 2017 4:17 AM');
  });
});
