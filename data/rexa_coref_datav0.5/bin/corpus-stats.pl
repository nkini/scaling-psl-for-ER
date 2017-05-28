#!/usr/bin/perl

# usage: corpus-stats.pl citation.xml

$f = $ARGV[0];

print &count(" id=", $f) . " entity mentions (author title venue institution)\n";

# Calculate cluster stats.
open IN, $f or die "can't read $f\n";
while (<IN>) {
    if (/clusterid=\"([0-9]+)\"/) {
	$ids{$1}++;
	$total++;
    }
}
$c = keys %ids;
print "$c clusters\n";
print "\tavg cluster size=" . (1.0 * $total / $c) . "\n";

$max = 0;
$min = $total + 1;
foreach $k (keys %ids) {
    if ($ids{$k} > $max) {
	$max = $ids{$k};
    }
    if ($ids{$k} < $min) {
	$min = $ids{$k};
    }    
}
print "\tmax cluster size $max\n";
print "\tmin cluster size $min\n";


# Calculate entity stats
print &count("<reference ", $f) . " citation mentions\n";
print &count("<headers ", $f) . " header mentions\n";
print &count("<author ", $f) . " authors\n";
print &count("<title ", $f) . " titles\n";
print &count("<journal |<conference |<booktitle ", $f) . " venues\n";
print &count("<institution ", $f) . " institutions\n";


# Count number of occurrences of a regular expression in a file.
sub count {
    my ($r, $f) = @_;
    $c = `egrep '$r' $f | wc -l`;
    chomp $c;
    return $c;
}

