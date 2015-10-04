####Assigenment 1

**Exercise 1. Data exploration using spark (30% of the grade)**

This exercise is to perform some data exploration of page view statistics for Wikimedia projects. The questions here are based on the Berkley Spark tutorial to be found at the following link. There, you can get good hints on how to solve these questions, [link](http://ampcamp.berkeley.edu/big-data-mini-course/data-exploration-using-spark.html).

First, you’ll need to download the page view statistics for 6pm on Dec 9, 2007 from [here](http://dumps.wikimedia.org/other/pagecounts-raw/2007/2007-12/pagecounts-20071209-180000.gz)

each line, delimited by a space, contains stats for one Wikimedia page. The schema is:
<project_code> <page_title> <num_hits> <page_size>
Use this statistics file to find answers to the following questions:

*First, launch the Spark shell and then create an RDD (Resilient Distributed Dataset) named pagecounts from the input files.*

```pyrhon
//fire up spark
./bin/pyspark
//create an RDD
pagecounts = sc.textFile("data/wiki/pagecounts")
```

*Let’s take a peek at the data. Use the take() operation of an RDD to get the first K records,
with K = 10. The take() operation returns an array and Scala simply prints the array with each
element separated by a comma. This is not easy to read. Make the output prettier by traversing
the array to print each record on its own line.*

```python
#take 10 lines
pagecounts.take(10)
#prettier output
for each in pagecounts.take(10):
	print each
```

*Check how many records in total are in this dataset.*

```python
pagecounts.count()
//856769
```

*Derive an RDD containing only English pages from pagecounts. This can be done by applying a filter function to pagecounts. For each record, we can split it by the field delimiter (i.e. a space) and get the first field and then compare it with the string “en”. (experiment in this and following questions with caching the RDDs using the “.cache” action).*

```python
enPages = pagecounts.filter(lambda x: x.split(" ")[0] == "en").cache()
```

*How many records are there for English pages?*

```python
enPages.count()
//389632
```

*Generate a histogram of total English page views on Wikimedia pages for the different titles included in the dataset. The high level idea of what you’ll be doing is as follows. First, generate a key value pair for each line; the key is the page title (the second field), and the value is the number of pageviews for that page (the third field).*

```python
enTuples = enPages.map(lambda x: x.split(" "))
enKeyValuePairs = enTuples.map(lambda x: (x[1], int(x[2])))
```

*Find English pages that were viewed more than 1000 times in the dataset*

```python
enKeyValuePairs.reduceByKey(lambda x, y: x + y, 1).collect()
result = enKeyValuePairs.filter(lambda x: x[1] > 1000)
for each in result.take(5):
	print each
```

*Please view the report.pdf*