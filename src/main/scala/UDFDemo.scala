import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.SaveMode.Overwrite
import com.snowflake.snowpark.types.{StringType, StructField, StructType}

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

// import org.apache.log4j.{Level, Logger}

/**
 * Demonstrates how to use Snowpark to create user-defined functions (UDFs)
 * in Scala.
 *
 * Before running the main method of this class, download the data and JAR files
 * needed for the the demo, then run the main method of the UDFDemoSetup class
 * to upload those files to internal stages.
 */
object UDFDemo {
  // The name of the internal stage for the demo data.
  val dataStageName = "snowpark_demo_data"

  // The name of the internal stage for the JAR files needed by the UDF.
  val jarStageName  = "snowpark_demo_udf_dependency_jars"

  // The name of the file containing the dataset.
  val dataFilePattern = "training.1600000.processed.noemoticon.csv"
  /*
   * Reads tweet data from the demo CSV file from a Snowflake stage and
   * returns the data in a Snowpark DataFrame for analysis.
   */
  def collectTweetData(session: Session): DataFrame = {
    // Import names from the implicits object, which allows you to use shorthand
    // to refer to columns in a DataFrame (e.g. `'columnName` and `$"columnName"`).
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
    // dataStageName is the name of the stage that was created
    // when you ran UDFDemoSetup earlier, and dataFilePattern is
    // the pattern matching the files that were uploaded to that stage.
    val origData = session
      .read
      .schema(StructType(schema))
      .option("compression", "gzip")
      .csv(s"@$dataStageName/$dataFilePattern")

    // Drop all of the columns except the column containing the text of the tweet
    // and return the first 100 rows.
    val tweetData = origData.drop('target, 'ids, 'date, 'flag, 'user).limit(100)

    Console.println("\n=== Retrieving the data and printing the text of the first 10 tweets")
    // Display some of the data.
    tweetData.show()

    // Return the tweet data for sentiment analysis.
    return tweetData
  }

  /*
   * Determines the sentiment of the words in a string of text by using the
   * Stanford NLP API (https://nlp.stanford.edu/nlp/javadoc/javanlp/).
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

  /*
   * Creates a user-defined function (UDF) for sentiment analysis. This function
   * registers the analyze function as a UDF, along with its dependency JAR files.
   */
  def createUDF(session: Session): UserDefinedFunction = {
    Console.println("\n=== Adding dependencies for your UDF ===\n")
    // Register CoreNLP libraries library JAR files as dependencies to support
    // the UDF. The JAR files are already in the Snowflake stage named by
    // jarStageName. The stage created and JARs uploaded when you ran code in
    // UDFDemoSetup.scala.
    session.addDependency(s"@$jarStageName/stanford-corenlp-3.6.0.jar.gz")
    session.addDependency(s"@$jarStageName/stanford-corenlp-3.6.0-models.jar.gz")
    session.addDependency(s"@$jarStageName/slf4j-api.jar.gz")
    session.addDependency(s"@$jarStageName/ejml-0.23.jar.gz")

    Console.println("\n=== Creating the UDF ===\n")
    // Register the analyze function as a UDF that analyzes the sentiment of
    // text. Each value in the column that you pass to the UDF is passed to the
    // analyze method.
    val sentimentFunc = udf(analyze(_))
    return sentimentFunc
  }

  /*
   * Analyzes tweet data, discovering tweets with a happy sentiment and saving
   * those tweets to a table in the database.
   */  
  def processHappyTweets(session: Session, sentimentFunc: UserDefinedFunction, tweetData: DataFrame): Unit = {
    // Import names from the `implicits` object so you can use shorthand to refer
    // to columns in a DataFrame (for example, `'columnName` and `$"columnName"`).
    import session.implicits._

    Console.println("\n=== Creating a transformed DataFrame that contains the results from calling the UDF ===\n")
    // Call the UDF on the column that contains the content of the tweets.
    // Create and return a new `DataFrame` that contains a "sentiment" column.
    // This column contains the sentiment value returned by the UDF for the text
    // in each row.
    val analyzed = tweetData.withColumn("sentiment", sentimentFunc('text))

    Console.println("\n=== Creating a transformed DataFrame with just the happy sentiments ===\n")
    // Create a new DataFrame that contains only the tweets with happy sentiments.
    val happyTweets = analyzed.filter('sentiment === 3)

    Console.println("\n=== Retrieving the data and printing the first 10 tweets ===\n")
    // Display the first 10 tweets with happy sentiments.
    happyTweets.show()

    Console.println("\n=== Saving the data to the table demo_happy_tweets ===\n")
    // Write the happy tweet data to the table.
    happyTweets.write.mode(Overwrite).saveAsTable("demo_happy_tweets")
  }

  /*
   * Uploads tweet data from a demo CSV, creates a UDF, then uses the UDF to
   * discover the sentiment of tweet text.
   */
  def discoverHappyTweets(session: Session): String = {
    // Collect tweet data from the demo CSV.
    val tweetData = collectTweetData(session)
    // Register a user-defined function for determining tweet sentiment.
    val sentimentFunc = createUDF(session)
    // Analyze tweets to discover those with a happy sentiment.
    val happyTweets = processHappyTweets(session, sentimentFunc, tweetData)

    "Complete"
  }

  def main(args: Array[String]): Unit = {
    // If you need to adjust the logging level of the Snowpark library,
    // uncomment this line and set the level to a different `Level` field.
    // Logger.getLogger("com.snowflake.snowpark").setLevel(Level.INFO)

    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.
    val session = Session.builder.configFile("snowflake_connection.properties").create

    // Upload and process the tweets.
    discoverHappyTweets(session)

    Console.println("\n=== Printing the first 10 rows of happy tweets table ===\n")
    session.table("demo_happy_tweets").show()
  }
}