open fpIn, "<", "classWiseTitleBody.txt";
open fpOut, ">", "classWiseTfIdf.txt";

while (<fpIn>) {
	chomp;
	@keyValueSplit = split(/\s*-->\s*/);
	@termSplit = split(/,\s/, $keyValueSplit[1]);
	#print "$keyValueSplit[0] --> $#termSplit\n";
	for ($i=0; $i<=$#termSplit;$i++) {
		@termFrequencySplit = split('@', $termSplit[$i]);
		if (($termFrequencySplit[0] ne "TotalWords")) {
			$dictionaryHashMap{$keyValueSplit[0]}{$termFrequencySplit[0]} = $termFrequencySplit[1];
		}
	}
}

foreach $class (keys %dictionaryHashMap) {
	foreach $term ( keys %{ $dictionaryHashMap{$class} } ) {
		if (exists $termClassFrequency{$term}) {
			$termClassFrequency{$term} += 1;
		} else {
			$termClassFrequency{$term} = 1;
		}
	}
}

foreach $class (keys %dictionaryHashMap) {
	$totalTfIdf = 0;
	print fpOut "$class -->\t";
        foreach $term ( keys %{ $dictionaryHashMap{$class} } ) {
		print fpOut "$term\@";
		$termFrequency = $dictionaryHashMap{$class}{$term};
		$NumberOfClasses = scalar keys %dictionaryHashMap;
		$classFrequency = $termClassFrequency{$term};
		$tfIdf = (1 + log($termFrequency))*(log(($NumberOfClasses)/($classFrequency)));
		print fpOut "$tfIdf, ";
		$totalTfIdf += $tfIdf;
        }
 	print fpOut "TotalTfIdf\@$totalTfIdf\n";
}


undef %dictionaryHashMap;
undef %termClassFrequency;
close fpIn;
close fpOut;
