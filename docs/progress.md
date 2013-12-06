# An Experiment into WARC Compression Techniques

The [Web ARChive file format][warcstandard] is a plain text format created originally by the [Internet Archive][ia] but now maintained by the [International Internet Preservation Consortium (IIPC)][iipc]. A WARC file consists of multiple records, each record describes a single crawl of a single URL. Records contain a descriptive metadata header made up of `key: value` pairs such as `WARC-Date: 2006-09-19T17:20:24Z` and `Content-Type: image/png`. Following the header is the content of the record, this can be any data type, including binary. The WARC ISO standard does not specify how to arrange WARC records in files, Heritrix, the de facto standard web archiver, simply appends each record in an "as it comes" fashion when it creates them. The standard does provide some advice on limiting file sizes and on file compression; limit each file to 1GB and compress the file using gzip. It is our belief that a less naïve approach to record structure, file organisation and data compression can lead to significant space savings and reduce record lookup times.

[warcstandard]: http://bibnum.bnf.fr/warc/WARC_ISO_28500_version1_latestdraft.pdf
[ia]: https://archive.org/
[iipc]: http://netpreserve.org/

## Hypotheses

1. Organising records by URL rather than creation date will reduce lookup speeds. In addition such a structure would be easier to maintain, navigate and understand without requiring additional tools. 
2. The WARC standard cites file system issues when splitting records into many files. Using a trie file and directory structure will reduce and/or eliminate these issues. 
3. Given the inherent similarity of content gained from consecutive crawls to the same URL, "content aware" compression and/or delta algorithms will perform better than a naïve gzip solution. 

## The Data

In order to test our hypotheses we have collected two sets of web data.

### Generated Data

This data set consists of randomly generated files that have been randomly and repeatedly altered. For each file we have a list of "changes over time" that simulate the changes made to web content. The [HTTP Archive][hhtpa] maintains information on the file types most commonly used in web sites, this list has been used to inform the types of data that has been generated for this data set: HTML, CSS, JavaScript, GIF, JPEG, and PNG. We have also create plain text files.

For each file type we randomly generate a file of that type and then apply a modification process to it. We repeat this process many times. Possible modification processes are:

	| File Type  | Modification       | Description                                                                 |
	| :--------- | :----------------- | :-------------------------------------------------------------------------- |
	| Plain text | delete             | Delete a block of characters at random from the text                        |
	|            | insert             | Insert a block of characters at random into the text                        |
	|            | substitute         | Replace a block of characters at random in the text                         |
	| CSS        | delete             | Delete random declarations from the CSS                                     |
	|            | insert declaration | Randomly sample a declaration and copy it to a random location              |
	|            | insert rule        | Copy a rule but replace its declarations with randomly sampled declarations |
	| Images     | scale              | Shrink or grow the image by a random amount                                 |
	|            | rotate             | Rotate the image by a random amount                                         |
	|            | crop               | Crop a randomly sized strip from the outside edge of the image              |
	|            | grey scale         | Make the picture black and white                                            |
	|            | modulate           | Reduce or increase the brightness, hue, and/or saturation of the image      |

NOTE: Billy: The plain text and css modifications are working, the image library I'm working with has thrown up some issues that I haven't fixed yet so these modifications don't work quite as advertised. I haven't yet implemented the HTML modifications, it will be a combination of the plain text and CSS processes. I'm unsure about how to randomly generate and modify javascript in a meaningful way. I'm not certain it would be necessary, given the other text options we'll have.

[httpa]: http://httparchive.org/

### Crawled Data from GitHub

This data set consists of every git repository hosted on [GitHub][github] with ['github.io' in the name][githubq]. GitHub have, since 2008, hosted webpages stored in git repositories. This data set is a comprehensive crawl of every such website. The benefit to crawling this data is that git provides us with the ability observe each website's changes over time by iterating through the repository's commits. We achieve this by cloning the repository, and iterating through every commit listed in the output of the `git rev-list` command. For each commit we serve the repository's contents using Jekyll (the software used by GitHub to create web pages from repositories) and run a  Heritrix crawl over the entire server.

We have crawled 27,907 repositories and are in the process of iterating through and archiving them. So far we have processed ~500 commits over 1 repository.

[github]: https://github.com/
[githubq]: https://github.com/search?q=github.io&type=Repositories&ref=searchresults

### Further Data Sets

It would be good if we could obtain a heritrix crawl from a 'real world' archivist. The generated data will obviously not display the same aggregate qualities as the World Wide Web, the GitHub data is very likely to display some bias towards tech-oriented websites and blogs. If we could use a library's archive as well then we can be more confident in our results. 

## The Experiments

Experiments for hypotheses 1 and 2 rely on the archived data from the github crawl and so haven't been run. The setup will be to try random access of WARC records in the plain heritrix data, then optimise the file layout and structure and try again. We're looking to time: access, `ls`, and search, as well as comparing file sizes and overall data directory size. 

Experiments on file compression are at an early stage. We have a system capable of analysing data and providing stats on how various compression techniques perform. This system is run over the output of the generated data modification processes. Currently the compression and delta algorithms considered are:

	| Name         | Compression/Delta | Description                                                                                                           |
	| :----------- | :---------------- | :-------------------------------------------------------------------------------------------------------------------- |
	| zip          | compression       | Uses the DELFATE algorithm, can compress multiple files                                                               |
	| bzip2        | compression       | Uses the Burrows-Wheeler algorithm to compress. This should be better compression but slower                          |
	| gzip         | compression       | Uses DEFLATE, can only compress a single file                                                                         |
	| bsdiff       | compression       | Binary data differencing tool that uses  Larsson and Sadakane's qsufsort for suffix sorting                           |
	| diff -e      | delta             | Solves the longest common subsequence problem, produces an edit script that can be understood by the `ed` application |
	| diff -e | gz | delta             | Takes the output of diff -e and gzips it                                                                              |
	| vcdiff       | delta             | Diffs by finding long common strings, particularly good for code differencing                                         |

Each compression algorithm is given every modified version of the data in a single file to compress. The delta algorithms produce deltas in a manner similar to that found in MPEG encoding. Each modified version of the data is compared to another to produce the delta, the comparison version can be 1, 2, 5, or 10 versions away from the current one. We also run a version where all deltas are taken from the base data. This comparison version is referred to as the iframe.

In addition to this list we analyse the data according to Shanon's Theory of Communications at various levels. This gives us a good idea of how well the compression algorithms are faring.

To this list we want to add our own compression/delta algorithms that take into consideration the type of data found in a web archive.

An example output from the experiment so far might be:

	encoding                 mean size    size variance    % of raw    % of best
	---------------------  -----------  ---------------  ----------  -----------
	9 order optimal            1057.00            57.64        1.59       100.00
	8 order optimal            1412.17            83.97        2.12       133.60
	diffe_gz, iframe @ 1       1542.92            69.54        2.32       145.97
	diffe_gz, iframe @ 2       1547.83            42.33        2.33       146.44
	diffe_gz, iframe @ 5       1694.58           288.63        2.55       160.32
	diffe_gz, iframe @ 10      1768.58           806.45        2.66       167.32
	gzip                       1850.92             3.17        2.78       175.11
	diffe_gz, iframe @ 0       1879.33          4654.24        2.83       177.80
	7 order optimal            1931.42            87.54        2.91       182.73
	zip                        1963.92             3.17        2.95       185.80
	6 order optimal            2424.75           190.75        3.65       229.40
	bz2                        2708.75            42.20        4.07       256.27
	5 order optimal            3632.08           217.72        5.46       343.62
	vcdiff, iframe @ 1         4137.75            66.39        6.22       391.46
	vcdiff, iframe @ 0         5300.33         35711.70        7.97       501.45
	bsdiff, iframe @ 1         6529.08            46.27        9.82       617.70
	4 order optimal            7022.17           157.24       10.56       664.35
	bsdiff, iframe @ 0         7192.67         14838.79       10.82       680.48
	vcdiff, iframe @ 10        7540.25          5804.57       11.34       713.36
	bsdiff, iframe @ 10        9488.50          4814.09       14.27       897.68
	vcdiff, iframe @ 5        13399.50            80.27       20.15      1267.69
	bsdiff, iframe @ 5        14950.50           243.36       22.49      1414.43
	3 order optimal           15787.58           670.99       23.75      1493.62
	diffe, iframe @ 1         17459.33        724073.52       26.26      1651.78
	vcdiff, iframe @ 2        19254.33            27.88       28.96      1821.60
	bsdiff, iframe @ 2        19851.25             3.30       29.86      1878.07
	diffe, iframe @ 2         22554.67        253335.15       33.93      2133.84
	2 order optimal           26591.00           273.27       40.00      2515.70
	diffe, iframe @ 5         31943.08       2152407.36       48.05      3022.05
	1 order optimal           35198.25            28.93       52.94      3330.01
	diffe, iframe @ 10        43770.75       7204592.02       65.84      4141.04
	0 order optimal           44227.00             0.00       66.52      4184.20
	diffe, iframe @ 0         55582.50      12403770.09       83.60      5258.51
	raw                       66483.00             0.00      100.00      6289.78

This experiment took some lorem ipsum text and ran a delete modifier over it, it deleted 1 character at a time, 20 times. The experiment was repeated 12 times.

## The Results

We can see that `diff -e | gz` performed very well on this data, coming within 1% of the 9th order optimal compression and compressing the data down to 2.32% of the original.

## The Conclusions

More work is required. 
