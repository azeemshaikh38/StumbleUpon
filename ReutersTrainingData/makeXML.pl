open fpTmp, ">", "reuters.xml";
close fpTmp;

@files = glob("reut2-*.sgm");

foreach my $file (@files) {
	open fpIn, "<", $file;
	open fpOut, ">>", "reuters.xml";

	while (<fpIn>) {
		chomp;
		if ($_ =~ /^<REUTERS/) {
			$start = 1;
			print fpOut "$_";
		} elsif ($_ =~ /^<\/REUTERS>/) {
			$start = 0;
			print fpOut "$_\n";
		} elsif ($start == 1) {
			print fpOut "$_";
		}
	}
	close fpIn;
	close fpOut;
}
