#!/usr/bin/perl

# Program to assign (virtual) Spark nodes to CPU's in
# a server. Use this program to configure virtual nodes
# in a Spark-HPC system.
# Usage:
# workers-per-socket.pl <--balance> <--debug> n_nodes
#
# Use the --balance option to calculate if there are
# CPU's not being used.

use strict;
use POSIX;
use Getopt::Long qw(GetOptions);

#################### VARIABLES #######################################
my $debug = 0;
my $usage = "Usage: $0 --balance <n_nodes>";

# Check if nodes are balanced
my $balance = 0;

GetOptions("balance" => \$balance, "debug" => \$debug)
  or die $usage;

my $n_nodes = $ARGV[0];
$n_nodes =~ m(\d+) or die $usage;
$n_nodes > 0 or die "n_nodes should be > zero";

#################### SUBROUTINES #####################################
sub read_cpus () {
  my %sockets = ();

  open CPUINFO, "</proc/cpuinfo" or die "cannot open /proc/cpuinfo: $!";
  my $cpu = -1;
  while (my $line = <CPUINFO>) {
    chomp $line;
    if ($line =~ m(^processor\s*:)) {
      $cpu = $line;
      $cpu =~ s(^processor\W*)(); $cpu =~ s(\W*$)();
    }
    if ($line =~ m(^physical id\s*:)) {
      my $socket = $line;
      $socket =~ s(^physical id\W*)(); $socket =~ s(\W*$)();
      push @{$sockets{$socket}}, $cpu;
    }
  }
  close CPUINFO;
  return %sockets;
}

sub print_sockets ($) {
  my ($sockets_ref) = @_;
  my %sockets = %{ $sockets_ref };
  for my $socket (sort keys %sockets) {
    print STDERR "socket $socket: ";
    for my $cpu ( @{$sockets{$socket}} ) {
      print STDERR "$cpu ";
    }
    print STDERR "\n";
  }
}

# how many cpu's per socket (a.k.a. socket)?
# check all sockets have same number of cpu's
sub calculate_cpus_per_socket($) {
  my ($sockets_ref) = @_;
  my %sockets = %{ $sockets_ref };
  my $n = 0; # sockets per cpu
  my $socket_n = 0;
  # verify all sockets have the same number of cpu's
  for my $socket (sort keys %sockets) {
    my @sockets_list = @{ $sockets{$socket}};
    my $n_current = @sockets_list;
    if ($n_current != $n && $socket_n) {
      die "invalid number of CPU's ($n_current vs $n) in socket $socket";
    }
    $n = $n_current;
    $socket_n ++;
  }
  return $n;
}

sub assign_nodes ($$) {
  my ($sockets_ref, $n_nodes) =@_;
  my %sockets = %{ $sockets_ref };
  my $n_sockets = scalar keys %sockets ;
  my $nodes_per_socket = ceil ($n_nodes / $n_sockets);
  my $cpus_per_socket = calculate_cpus_per_socket(\%sockets);
  my $cpus_per_node = floor ($cpus_per_socket / $nodes_per_socket);

  $debug and print STDERR "N sockets: $n_sockets NODES PER: $nodes_per_socket ($cpus_per_node CPUs)\n";
  my $cpu = 0;
  my $socket = 0;
  my $node_in_socket = 0;
  for (my $i = 0 ; $i< $n_nodes; $i++) {
    my @socket = @{$sockets{$socket}};
    # print "<<<($socket)" + join(" ", @socket)+ ">>>\n";
    print "$i:$socket:";
    for (my $node_cpu = 0; $node_cpu<$cpus_per_node; $node_cpu++) {
      print "$socket[$cpu]";
      ($node_cpu < $cpus_per_node -1) and print ",";
      $cpu++;
    }
    print "\n";
    $node_in_socket++;
    if ($node_in_socket == $nodes_per_socket) { $socket++; $cpu=0; $node_in_socket = 0;}
  }
}


#################### MAIN PROGRAM ####################################
my %sockets = read_cpus();

$debug and print_sockets(\%sockets);

if ($balance) {
  my $cpus_per_socket = calculate_cpus_per_socket(\%sockets);
  my $n_sockets = scalar keys %sockets ;
  my $nodes_per_socket = ceil ($n_nodes / $n_sockets);
  my $cpus_per_node = floor ($cpus_per_socket / $nodes_per_socket);

  my $extra_cpus = ($n_sockets*$cpus_per_socket) - ($n_nodes * $cpus_per_node);
  if ($extra_cpus) {
    print STDERR "Warning: CPU's not uniformly divided ($extra_cpus unassigned)\n";
  }

}
else {
  assign_nodes(\%sockets, $n_nodes);
}


