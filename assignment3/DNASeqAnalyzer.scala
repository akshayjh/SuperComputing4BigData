import java.io.{FileWriter, BufferedWriter, PrintWriter, File}

import org.apache.spark.{SparkContext, SparkConf}
object DNASeqAnalyzer
{
  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)


    var t0 = System.currentTimeMillis

    // Main code here ////////////////////////////////////////////////////
    //read data
    //data source
    val data1 = sc.textFile("/Users/wb/Downloads/data/spark/fastq1.fq")
    val data2 = sc.textFile("Users/wb/Downloads/data/spark/fastq2.fq")
    //file name
    val fileName = "chunk"
    val fileSuffix = ".fq"
    //add index for each rdd
    val kvRdd1 = data1.zipWithIndex().map(line => (line._2, line._1)).map(line => line._1/4 -> line._2).groupByKey().sortBy(line => line._1)
    val kvRdd2 = data2.zipWithIndex().map(line => (line._2, line._1)).map(line => line._1/4 -> line._2).groupByKey().sortBy(line => line._1) //combine 2 rdds
    //union 2 rdds as a whole and reduce by key
    val kvRdd = (kvRdd1 union kvRdd2).reduceByKey(_++_)
    val kvRddFinal = kvRdd.sortBy(line => line._1).partitionBy(new org.apache.spark.HashPartitioner(8))
    //generate 8 sub chunk document, from chunk0.fq to chunk7.fq
    val rdd = kvRddFinal.mapPartitionsWithIndex{(index, iter) =>
      val writer = new PrintWriter(new File("/home/bowang/Assignment3/question1" + fileName + index + fileSuffix))
      while(iter.hasNext){
        val contentRdd = iter.next._2
        contentRdd.foreach(writer.println)
      }
      writer.close()
      (iter)
    }.collect()
    //Main code end
    val et = (System.currentTimeMillis - t0) / 1000
    val mins = et / 60
    val secs = et % 60
    println( "{Time taken = %d mins %d secs}".format(mins, secs) )
  }
}
