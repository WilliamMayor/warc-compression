# Web Archive Compression Comparison

The Web ARChive format (WARC) is an ISO standard for human-readable files that comprise a web archive. In this paper we explore techniques for optimally compressing these files. We demonstrate that a combination of differencing and compression algorithms can reduce the total archive size to X%, improving by a factor of X on the recommended strategy.

## Introduction

* Web archiving generates lots of data
* web archives are stored in WARC files
* ISO WARC standard recommends
	- fill files until 1GB limit
	- compress files using gzip
	- record unchanging responses using revisit records
* using these recommendations on our data we achieve X% compression
* in this paper we explore strategies for compressing
* we have the following goals
	- reduce total archive size
	- reduce WARC record size
	- allow easy partitioning of data
	- increase ease at which archive is human understandable/explorable

## Background and Related Work

* WARC file format
* gzip, bzip2, zip, tar
	- why these?
* vcdiff, bsdiff, diffe
	- why these?
* internet archive, british library IIPC collections

## Generated Data

* take 1MB of lore ipsum data
* apply changes over time
* compress data and compare

## GitHub Data

* real world websites
* *every* change
* tech bias
* heritrix processed
* compress data and compare

## British Library Data

* use british library archive
* compress and compare

## Website Changes Over Time

* website composition
	- content type
	- average size
	- file counts (links per page, per domain)
	- changes over time (per domain, globally)
* page contents
	- change rate
	- by content type
	- size difference
	- string distances (image distance?)
	- how often to crawl to get every change