# warc-compression

An experiment into Web ARCchive (WARC) file compression.

Current web archive crawlers (notably Heritrix) stream crawled data into WARC files as they are encountered. WARC files are added to until they reach a size limit (1GB is recommended in the standard; ISO 28500:2009). This project investigates changing this behaviour in order to achieve a higher rate of compression on the crawled data. It is our belief that organising the data according to URL will allow for much greater compression. We are also interested in exploring which compression algorithm is most suitable for this use case.

Traditionally, WARC files are organised this way because the sheer volume of data makes structuring the files infeasible. Specifically, creating a new file for each URL quickly overloads the operating system's ability to cope. We are interested in fully exploring and quantifying this trade-off. Hopefully, a good compromise can be found that allows for good compression and good file handling.

## Getting Set Up

The project is written in Python 2.7 and has a few external dependencies. The recommended way to get everything installed and running is to clone to repo and create a [virtual environment](https://pypi.python.org/pypi/virtualenv) to house Python and the requirements. Assuming you have [virtualenv](https://pypi.python.org/pypi/virtualenv) installed, the procedure should look something like this:

    $ git clone https://github.com/WilliamMayor/warc-compression.git
    $ cd warc-compression
    $ virtualenv venv
    $ . venv/bin/activate
    $ pip install -r requirements.txt
    $ python setup.py develop

You should now be ready to run some experiments.

## Compression Algorithm Performance

One of the first sets of experiment we are interested in explores the performance of compression algorithms. We are interested in how the algorithms compare across a variety of common web file formats. Currently the WARC standard recommends compressing each file using gzip, it also recommends that each WARC record be individually compressed using gzip to allow for better random access of the file. Here we question whether gzip is the optimal choice.

### Randomly Generated Data

The first type of experiment randomly generates data from a base file using a specified modifier function. This data is then concatenated into a single file and compressed using many different types of compression.

| Base Files       | Encodings  | Modifiers     |
| ---------------- | ---------- | ------------- |
| Lorem ipsum text | zip        | identity      |
| Sonnets          | bzip2      | reduce by one |
| HTML             | gzip       | reduce by 10  |
| CSS              | bsdiff     |               |
| Javascript       | diff -e    |               |
| GIF              | diff -e gz |               |
| PNG              | vcdiff     |               |
| JPG              |            |               |

To run an experiment choose a modifier and a base file and run, depending on the base data file and the number of repetitions of the experiment, this might take some time. The general command to run experiment looks like:

    python EXPERIMENT.PY BASE_FILE DATA_DIR 

Here's an example of how to run an experiment that takes a couple of paragraphs of lorem ipsum text and randomly deletes a single character at a time from the text 20 times:

    python CLONE_PATH/warcompress/experiments/single_char_delete.py CLONE_PATH/data/lorem.txt CLONE_PATH/data/lorem/

## Modifiers

The `warcompress.modifiers` package provides many modules that randomly modify data. They can do this in an intelligent way; understanding the format and content of the data and modifying whilst keeping the data valid.

### Text Modifiers

Text modifiers have very little understanding of the text they modify. They randomly select character positions in the text and modify in single groups of characters. For instance, the delete modifier will remove a single block of characters at a time. The `modify` method takes a second argument that represents the percentage of the original text to modify. This argument defaults to 1%.



