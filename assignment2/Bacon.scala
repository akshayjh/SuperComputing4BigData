// Two authors work on this code
// Author 1: Bo Wang
// Author 2: Jian Fang

/* Bacon.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the actors.list file
	
	def main(args: Array[String]) 
	{
		val cores = args(0)
		val inputFile = args(1)
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		conf.set("spark.storage.memoryFraction","0.4")
		val sc = new SparkContext(conf)
		
		println("Number of cores: " + args(0))
		println("Input file: " + inputFile)
		
		var t0 = System.currentTimeMillis

		// Main code here ////////////////////////////////////////////////////
		// ...
		//////////////////////////////////////////////////////////////////////
		
		// Write to "actors.txt", the following:
		//	Total number of actors
		//	Total number of movies (and TV shows etc)
		//	Total number of actors at distances 1 to 6 (Each distance information on new line)
		//	The name of actors at distance 6 sorted alphabetically (ascending order), with each actor's name on new line

		
		//step 1 generate a RDD key-value pair with newAPIHadoopFile
		// RDD of <actor, movieList>
		val hadoopConf = new Configuration(sc.hadoopConfiguration)
	    	hadoopConf.set("textinputformat.record.delimiter", "\n\n")
		val rdd = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat],classOf[LongWritable], classOf[Text], hadoopConf).map { case (_, text) => text.toString}
		

		//step 2 generate a new RDD <actor, movie>
		val actorsMovies = rdd.map(s => (s.split("\t")(0), s.split("\t",2)(1))).flatMapValues(s => s.split("\n").map(_.trim)).mapValues(s => s.split("  ")(0))
		val moviesActors = actorsMovies.map(m => (m._2,m._1))

		val actorsNumber = actorsMovies.groupByKey().count
		val moviesNumber = moviesActors.groupByKey().count
		// Results of total actors number and total movies number
		println("the total actors number is " + actorsNumber)
		println("the total movies number is " + moviesNumber)

		//step 3 , make a RDD with actors and collaborating actors <actor, actors>
		val actorsActors = moviesActors.join(moviesActors).map(s => s._2).filter(n => (n._1 != n._2)).distinct

		//map <actor,collaboratingActor>
		val actorsActorsList = actorsActors.groupByKey()

		// add KevinBacon and distance of 0 to the actorDistance RDD
		val KbRdd = sc.makeRDD(Seq((KevinBacon,0)))

		//-------------------------------------------------------//


		// new <actor,distance> pair for itoration
		// join and get <actor, <distance, <actorList>>>, get <distance, <actorList>> elements, map to <actor, distance+1>
		// reduce the minimun distance of each key
		// iterate for 6 times to get distance from 1 to 6
		// Results of number of actors with distance of 1 to 6

		val actorDistanceEqualTo1 = actorsActorsList.filter(m => (m._1 == KevinBacon)).flatMapValues(s => s.iterator).map(s => (s._2,1))
		val newActorDistance1 = KbRdd.union(actorDistanceEqualTo1) // no need to reduce because it only contain actors of distance 1 and KevinBacon
		val actorNumberDistance1 = newActorDistance1.filter(m => (m._2 == 1)).count
		println("Number of actors with distance of 1 : " + actorNumberDistance1)

		val actorDistanceEqualTo2 = newActorDistance1.filter(m => (m._2 == 1)).join(actorsActorsList).map(m => m._2).flatMapValues(s => s.iterator).map(s => (s._2,2))
		val newActorDistance2 = newActorDistance1.union(actorDistanceEqualTo2).reduceByKey((m,n) => math.min(m,n))
		val actorNumberDistance2 = newActorDistance2.filter(m => (m._2 == 2)).count
		println("Number of actors with distance of 2 : " + actorNumberDistance2)

		val actorDistanceEqualTo3 = newActorDistance2.filter(m => (m._2 == 2)).join(actorsActorsList).map(m => m._2).flatMapValues(s => s.iterator).map(s => (s._2,3))
		val newActorDistance3 = newActorDistance2.union(actorDistanceEqualTo3).reduceByKey((m,n) => math.min(m,n))
		val actorNumberDistance3 = newActorDistance3.filter(m => (m._2 == 3)).count
		println("Number of actors with distance of 3 : " + actorNumberDistance3)

		val actorDistanceEqualTo4 = newActorDistance3.filter(m => (m._2 == 3)).join(actorsActorsList).map(m => m._2).flatMapValues(s => s.iterator).map(s => (s._2,4))
		val newActorDistance4= newActorDistance3.union(actorDistanceEqualTo4).reduceByKey((m,n) => math.min(m,n))
		val actorNumberDistance4 = newActorDistance4.filter(m => (m._2 == 4)).count
		println("Number of actors with distance of 4 : " + actorNumberDistance4)

		val actorDistanceEqualTo5 = newActorDistance4.filter(m => (m._2 == 4)).join(actorsActorsList).map(m => m._2).flatMapValues(s => s.iterator).map(s => (s._2,5))
		val newActorDistance5 = newActorDistance4.union(actorDistanceEqualTo5).reduceByKey((m,n) => math.min(m,n))
		val actorNumberDistance5 = newActorDistance5.filter(m => (m._2 == 5)).count
		println("Number of actors with distance of 5 : " + actorNumberDistance5)

		val actorDistanceEqualTo6 = newActorDistance5.filter(m => (m._2 == 5)).join(actorsActorsList).map(m => m._2).flatMapValues(s => s.iterator).map(s => (s._2,6))
		val newActorDistance6 = newActorDistance5.union(actorDistanceEqualTo6).reduceByKey((m,n) => math.min(m,n))
		val nameWithDistance6 = newActorDistance6.filter(m => (m._2 == 6)).sortByKey().map(m => m._1)
		val actorNumberDistance6 = nameWithDistance6.count
		println("Number of actors with distance of 6 : " + actorNumberDistance6)

		val rateOfSixDegree = (actorNumberDistance1 + actorNumberDistance2 + actorNumberDistance3 + actorNumberDistance4 + actorNumberDistance5 + actorNumberDistance6) * 10000 / actorsNumber
		println("The rate of six degree is " + rateOfSixDegree / 100 + "." + rateOfSixDegree % 100 + "%")

		// Name of actors with distance of 6, sort by alphabet in ascending order
		nameWithDistance6.foreach(println)

		val et = (System.currentTimeMillis - t0) / 1000
		val mins = et / 60
		val secs = et % 60
		println( "{Time taken = %d mins %d secs}".format(mins, secs) )
	} 
}
