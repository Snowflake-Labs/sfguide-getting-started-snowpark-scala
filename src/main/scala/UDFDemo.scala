import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import java.util.Properties

import com.snowflake.snowpark.SaveMode.Overwrite
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.log4j.{Level, Logger}

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

    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.
    val session = Session.builder.configFile("snowflake_connection.properties").create

    // Import names from the implicits object, which allows you to use shorthand to refer to columns in a DataFrame
    // (e.g. `'columnName` and `$"columnName"`).
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
    val tweetData = origData.drop('target, 'ids, 'date, 'flag, 'user).limit(100)

    Console.println("\n=== Retrieving the data and printing the text of the first 10 tweets")
    // Display some of the data.
    tweetData.show()

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
    val sentimentFunc = udf(analyze(_))

    Console.println("\n=== Creating a transformed DataFrame that contains the results from calling the UDF ===\n")
    // Call the UDF on the column that contains the content of the tweets.
    // Create and return a new DataFrame that contains column named "sentiment".
    // This column contains the value returned by the UDF for the text in each row.
    val analyzed = tweetData.withColumn("sentiment", sentimentFunc('text))

    Console.println("\n=== Creating a transformed DataFrame with just the happy sentiments ===\n")
    // Create a new DataFrame that contains only the tweets with happy sentiments.
    val happyTweets = analyzed.filter('sentiment === 3)

    Console.println("\n=== Retrieving the data and printing the first 10 tweets ===\n")
    // Display the first 10 tweets with happy sentiments.
    happyTweets.show()

    Console.println("\n=== Saving the data to the table demo_happy_tweets ===\n")
    happyTweets.write.mode(Overwrite).saveAsTable("demo_happy_tweets")

    Console.println("\n=== Printing the first 10 rows of that table ===\n")
    session.table("demo_happy_tweets").show()
  }
}
