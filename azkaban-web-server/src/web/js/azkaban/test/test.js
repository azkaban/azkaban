function addClass(el, newClass) {
 if(el.className.indexOf(newClass) === -1) {
   el.className += newClass;
 }
}
var chai = require('chai');

var assert = chai.assert;
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
