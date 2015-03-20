#!/usr/bin/env bash

apt-get update
apt-get install -y nodejs

su -c 'bash /vagrant/vagrant/bootstrap-ruby.sh' vagrant
