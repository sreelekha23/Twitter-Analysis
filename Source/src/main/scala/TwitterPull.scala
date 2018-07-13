
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by sange on 11/10/2016.
  */

object TwitterPull{


  def main(args: Array[String]) {
    val AccessToken = "790254444306833408-VUXVM722Hj6CdIUaCycBFzKbA3WGnmO";
    val AccessSecret = "kHDXSiv3ZTpS3h966nlnZkbd5dZzBJtl49aU5r4sLTpZ2";
    val ConsumerKey = "71GFiDzYipT3O3rVRTQOuQMiT";
    val ConsumerSecret = "8YnMMwT1NxQEHLnyrlExYiGMamVfbDYR8fy219JovwpMaXvicy";

    val consumer = new CommonsHttpOAuthConsumer(ConsumerKey, ConsumerSecret);
    consumer.setTokenWithSecret(AccessToken, AccessSecret);
    System.setProperty("hadoop.home.dir","C:\\Users\\sange\\Downloads\\hadooponwindows-master\\hadooponwindows-master")
    val sparkConf = new SparkConf().setAppName("TwitterPull").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df=sqlContext.read.json("C:\\Users\\sange\\Desktop\\Pavan\\data2.json")
    df.registerTempTable("dftable")
    println("The Users with their followers count in Descending Order")
    val active=sqlContext.sql("select user.screen_name as screenname, max(user.followers_count),user.location as location from dftable where user.location is not NULL and user.followers_count>100 group by user order by user.followers_count desc")
      active.show(150)
    println("Enter any screen name name from the list above You will get the followers list of that particular user")
    val screenname=scala.io.StdIn.readLine()
    val request = new HttpGet("https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+screenname);
    consumer.sign(request);
    val client = new DefaultHttpClient();
    val response = client.execute(request);
    println(IOUtils.toString(response.getEntity().getContent()));
  }
}
