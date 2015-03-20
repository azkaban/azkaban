Azkaban github page source
========

View the page at [Azkaban Github Page](http://azkaban.github.io/azkaban).

Running Locally
========

###Using Vagrant
Ensure you have Vagrant and VirtualBox install on your machine.

Start the virtual machine.

  	vagrant up

Once the VM is ready

  	vagrant ssh
  	cd /vagrant
  	bundle exec jekyll serve

Then view the page at http://localhost:4000.

###Local machine
One can follow the guide [here](https://help.github.com/articles/using-jekyll-with-pages/) or follow the steps below.

Install bundler.

  	gem install bundler

Create a `Gemfile`.

  	source 'https://rubygems.org'
  	gem 'github-pages'

Run the commands.

  	bundle install
  	bundle exec jekyll serve

Then view the page at http://localhost:4000.

