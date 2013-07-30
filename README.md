#TwitterAnalysis
=============================================================================

Not under active development since September 2012.
-----------------------------------------------------------------------------

TwitterAnalysis is a text analysis program I developed during research for my Masters.
It provides an analysis of the trending topics within a 12GB body of Tweets (16 million tweets) 
and was built from the ground up to pre-process, index and analyse microblog data.

The practical purpose of this research was to test the hypothesis that term clusters containing significant 
change in the term sets can be used in providing a measure of the volatility level associated with topics.
These volatility levels may exhibit meaningful trends over time and thus provide valuable data to Information Retrieval researchers.
Three particular trends are assessed by this tool:

1. Stable term clusters - term clusters that remain static over time.
2. Gradually trending term clusters - clusters possessing significant divergence in topic over time, (but retain an overall theme).
3. Sudden significant change - clusters that contain significant localized departure from the baseline term set.

-----------------------------------------------------------------------------

Development and testing were initially conducted on my laptop and later moved to Amazon EC2 to meet memory requirement.
Some issues faced during development:
* Algorithmic complexity: with a complexity, Big-Θ, of n^2 when generating term clusters, careful consideration was given 
to the removal of stop words and other removable terms according to the distribution of terms in a corpus [Zipf's law] 
(http://nlp.stanford.edu/IR-book/html/htmledition/zipfs-law-modeling-the-distribution-of-terms-1.html)
* Memory management: memory constraints at the pre-processing stage required upto 16GB ram to complete processing, following this stage, 
retained terms shrank to below 20% of original volume, improving completion times for subsequent stages. Tweaks to the JVM yielded 
positive results while developing on a latop(2GB ram) but became insignificant when testing was moved to a larger machine

-----------------------------------------------------------------------------

Twitter data used during this research is available at [trec.nist.gov/data/tweets/] (http://trec.nist.gov/data/tweets/)


