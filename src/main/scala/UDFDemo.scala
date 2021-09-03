import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

import java.util.Properties
import com.snowflake.snowpark.SaveMode.Overwrite
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.log4j.{Level, Logger}

import java.time.{Duration, LocalDateTime}

/**
 * Demonstrates how to use Snowpark to create user-defined functions (UDFs) in Scala.
 *
 * Before running the main method of this class, download the data and JAR files needed for the
 * the demo, and run the main method of the UDFDemoSetup class to upload thoes files to internal
 * stages.
 */
object UDFDemo {
  /*
   * Determine the sentiment of the words in a string of text. This method uses the Stanford NLP
   * API (https://nlp.stanford.edu/nlp/javadoc/javanlp/) to determine the sentiment.
   * "The predicted classes can be an arbitrary set of non-negative integer classes,
   * but in our current sentiment models,
   * the values used are on a
   * 5-point scale of 0 = very negative, 1 = negative, 2 = neutral, 3 = positive, and 4 = very positive."
   */
  def analyze(text: String): Int = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
    lazy val pipeline = new StanfordCoreNLP(props)
    lazy val annotation = pipeline.process(text)
    annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).forEach(sentence => {
      lazy val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      return RNNCoreAnnotations.getPredictedClass(tree)
    })
    0
  }

  def main(args: Array[String]): Unit = {
    // If you need to adjust the logging level of the Snowpark library,
    // uncomment this line and set the level to a different `Level` field.
    // Logger.getLogger("com.snowflake.snowpark").setLevel(Level.INFO)

    val start = LocalDateTime.now()
    Console.println(s"\n=== Creating the session $start ===\n")
    // Create a Session that connects to a Snowflake deployment.
    val session = Session.builder.configFile("snowflake_connection.properties").create

    // Import syntax, which allows you to use shorthand to refer to columns in a DataFrame
    // (e.g. `'columnName` and `$"columnName"`), as well as .show() etc
    import session.implicits._

    Console.println("\n=== Setting up the DataFrame for the data in the stage ===\n")
    // Define the schema for the CSV file containing the demo data.
    val schema = Seq(
      StructField("target", StringType),
      StructField("ids", StringType),
      StructField("date", StringType),
      StructField("flag", StringType),
      StructField("user", StringType),
      StructField("text", StringType),
    )

    // Read data from the demo file in the stage into a Snowpark DataFrame.
    // UDFDemoSetup.dataStageName is the name of the stage that was created when you ran
    // UDFDemoSetup earlier, and UDFDemoSetup.dataFilePattern is the pattern matching the files
    // that were uploaded to that stage.
    val origData = session
      .read
      .schema(StructType(schema))
      .option("compression", "gzip")
      .csv(s"@${UDFDemoSetup.dataStageName}/${UDFDemoSetup.dataFilePattern}")
    // Drop all of the columns except the column containing the text of the tweet
    // and return the first 100 rows.
    val tweetData = origData.drop('target, 'ids, 'date, 'flag, 'user).limit(10000)

    Console.println("\n=== Retrieving the data and printing the text of the first 10 tweets")
    // Display some of the data.
    tweetData.show(10)

    Console.println("\n=== Adding dependencies for your UDF ===\n")
    // Next, create a user-defined function (UDF). This function uses the CoreNLP libraries, so you'll need to add the JAR files
    // for these libraries as dependencies.
    // UDFDemoSetup.jarStageName is the name of the stage that was created when you ran
    // UDFDemoSetup earlier.
    session.addDependency(s"@${UDFDemoSetup.jarStageName}/stanford-corenlp-3.6.0.jar.gz")
    session.addDependency(s"@${UDFDemoSetup.jarStageName}/stanford-corenlp-3.6.0-models.jar.gz")
    session.addDependency(s"@${UDFDemoSetup.jarStageName}/slf4j-api.jar.gz")
    session.addDependency(s"@${UDFDemoSetup.jarStageName}/ejml-0.23.jar.gz")

    Console.println("\n=== Creating the UDF ===\n")
    // Create a UDF that analyzes the sentiment of a string of text.
    // Each value in the column that you pass to the UDF is passed to the analyze method.
    val sentimentFunc = udf[Int, String](analyze)

    Console.println("\n=== Creating a transformed DataFrame that contains the results from calling the UDF ===\n")
    // Call the UDF on the column that contains the content of the tweets.
    // Create and return a new DataFrame that contains column named "sentiment".
    // This column contains the value returned by the UDF for the text in each row.
    val analyzed = tweetData.withColumn("sentiment", sentimentFunc('text))
    // There doesn't seem to be a way to cache or persist data.
    // Matierializing analyze causes the analysis to be run multiple times.
    analyzed.write.mode(Overwrite).saveAsTable("tweets_sentiment")
    val tweetSentiments = session.sql("select * from tweets_sentiment")
    tweetSentiments.groupBy("sentiment").count().show()

    Console.println("=== count groups")

    saveTableAndPrint(session, tweetSentiments, "positive_tweets", 3)
    saveTableAndPrint(session, tweetSentiments, "very_positive_tweets", 4)
    val end = LocalDateTime.now()
    val duration = DurationFormatUtils.formatDurationHMS(Duration.between(start, end).toMillis)
    Console.println(s"=== Finish at $end after $duration ===")

  }

  private def saveTableAndPrint(session: Session, analyzed: DataFrame, tableName: String, sentiment: Int): Unit = {
    import session.implicits._
    Console.println(s"\n=== Creating a transformed DataFrame with just the sentiments of $sentiment ===\n")
    val positiveTweets = analyzed.filter('sentiment === sentiment)
    Console.println(s"\n=== Retrieving the data and printing the first 10 tweets with sentiment $sentiment ===\n")
    // Display the first 5 tweets with happy sentiments.
    positiveTweets.show(5)

    Console.println(s"\n=== Saving the data to the table $tableName ===\n")
    positiveTweets.write.mode(Overwrite).saveAsTable(tableName)

    Console.println("\n=== Printing the first 10 rows of $tableName ===\n")
    session.table(tableName).show()
  }
}
