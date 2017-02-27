#!/usr/bin/env perl

# A script to get the VIP name of the current host.

use strict;
use warnings;

# Get a list of IP addresses that the host has.
sub get_ip_addrs() {
    my @ip_addrs = ();
    # Use the absolute path of ifconfig
    # since the script may be invoked without PATH being set.
    for my $line (`/sbin/ifconfig -a`) {
        # Parse the output of the ifconfig command
        # looking for patterns of IP addresses
        if ($line =~ /.*inet addr:(\d+\.\d+\.\d+\.\d+).*/) {
            push @ip_addrs, $1;
        }
    }
    @ip_addrs;
}

sub get_hostname_for_ip {
    my ($ip) = @_;
    my $result = `getent hosts $ip`;
    chomp($result);
    (split(/ /, $result))[-1];
}

sub get_vip_hostname {
    my $vip_hostname;
    for my $ip (get_ip_addrs()) {
        next if $ip eq '127.0.0.1';
        $vip_hostname = get_hostname_for_ip($ip);
        next if $vip_hostname =~ /hcl\d+/;
    }
    $vip_hostname;
}

sub get_shortname {
    my ($name) = @_;
    (split(/\./, $name))[0];
}

print get_vip_hostname(), "\n";