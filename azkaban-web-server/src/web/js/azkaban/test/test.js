function addClass(el, newClass) {
 if(el.className.indexOf(newClass) === -1) {
   el.className += newClass;
 }
}

var rewire = require('rewire'),
    dateJs = rewire('../util/date.js');

var chai = require('chai');
var assert = chai.assert;

//Some Test instances
describe('addClass', function() {
 it('should add class into element', function() {
   var element = { className: '' };

   addClass(element, 'test-class');

   assert.equal(element.className, 'test-class');
 });

 it('should not add a class which already exists in element', function() {
   var element = { className: 'exists' };

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
describe('CronTransformation', function() {

  var testStrFromCronToQuartz = dateJs.__get__('transformFromQuartzToUnixCron');

  it('should transfer correctly', function() {

   assert.equal(testStrFromCronToQuartz('0 3 * * 5'), '0 3 * * 4');
   assert.equal(testStrFromCronToQuartz('0 3 * * 5-7'), '0 3 * * 4-6');
   assert.equal(testStrFromCronToQuartz('0 3 * * 1,3-5 2016'), '0 3 * * 0,2-4 2016');
  });
});
