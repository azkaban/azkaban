/* After running `gradle clean installDist` this will get called by running `npm test` in the azkaban-web-server
   directory.  These tests are used against a running Azkaban Solo server to confirm version changes to javascript
   libraries don't break application web pages.

   todo li-afaris: `gradle npm_test` will look at package.json and call this test, but the test fails due because mocha
   can't be found.  It would be nice to fix the npm path when called by gradle.
*/

var chai   = require('chai');
var chaiHttp = require('chai-http');
var server = 'http://localhost:8081';
var should = chai.should();

chai.use(chaiHttp);

// Test that Azkaban web interface is running
describe("Will Azaban Login Work?", function() {

    it(' "/" returns 200 and is HTML ', function (done) {
      chai.request(server)
          .get('/')
          .end(function (err, res) {
            res.should.have.status(200);
            res.should.be.html;
            done();
          });
    });

    it('Should return sessionID after Posting login credentials', function (done) {
      chai.request(server)
          .post('/')
          .type('form')
          .send({'username':'azkaban', 'password': 'azkaban', 'action': 'login'})
          .end(function (err, res) {
            res.should.have.status(200);
            res.should.have.cookie('azkaban.browser.session.id');
            done();
          });
    });

});

