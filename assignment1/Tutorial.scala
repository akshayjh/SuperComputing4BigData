import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import TutorialHelper._

object Tutorial {
  def main(args: Array[String]) {
    //define checkpoint
    val checkpointDir = TutorialHelper.getCheckpointDirectory()
    //twitter api
    val apiKey = "ErKWTGkbB1GCi0jpruA9zs7mf"
    val apiSecret = "bg8yusuSRFPvDIrtMO5s1UcwPSgcwKHoehqwuc74YLRKcXLyLM"
    val accessToken = "2277305762-Ft5G0RncltTkfyhGXcvbYVryTwEdPEnGwdru3B0"
    val accessTokenSecret = "6Mgl9Wg2Sv3Y6Js2IemZt0VDQ5RlTPtXq3JeYkHv27Ixn"
    TutorialHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    //twitter streaming
    val ssc = new StreamingContext(new SparkConf(),Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None)
    //filter twitter that are retweet, return twitter content
    val retweetStream = tweets.filter(_.isRetweet).map { status => 
        (
        //get original tweet text
        status.getRetweetedStatus().getText(),
        //get original tweet retweet count
        status.getRetweetedStatus().getRetweetCount()
        )
    }
    val counts = retweetStream.countByValueAndWindow(Seconds(60), Seconds(1))
    val sortedCounts = counts.map { case(text, count) => (count, text) }
                         .transform(rdd => rdd.sortByKey(false))
    sortedCounts.foreach(rdd =>
        println("\nTop 5 retweets:\n" + rdd.take(5).mkString("\n")))
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}


