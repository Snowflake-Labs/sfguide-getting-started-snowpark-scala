import com.snowflake.snowpark._

// Imported for adjusting the logging level.
import org.apache.log4j.{Level, Logger}                                                             

/**
 * Connects to a Snowflake database and prints a list of tables in the database to the console.
 *
 * You can use this class to verify that you set the connection properties correctly in the
 * snowflake_connection.properties file that is used by this code to create a session.
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    // By default, the library logs INFO level messages.
    // If you need to adjust the logging levels, uncomment the statement below, and change X to
    // the level that you want to use.
    // (https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html)
    // Logger.getLogger("com.snowflake.snowpark").setLevel(Level.X)
 
    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.
    val session = Session.builder.configFile("snowflake_connection.properties").create

    Console.println("\n=== Creating a DataFrame to execute a SQL statement ===\n")
    // Create a DataFrame that is set up to execute the SHOW TABLES command.
    val df = session.sql("show tables")

    Console.println("\n=== Execute the SQL statement and print the first 10 rows ===\n")
    // Execute the SQL statement and print the first 10 rows returned in the output.
    df.show()

    // Note that because sql() returns a DataFrame, you can use method chaining
    // (https://en.wikipedia.org/wiki/Method_chaining) to perform the same calls in a single line:
    //
    //   session.sql("show tables").show()
    //
    // Also note that if you want to test the connection by querying one of your tables, you can
    // use the tables() method to query the table, rather than specifying a SQL statement. e.g.
    //
// Depending on the schema ....
//    Console.println("\n=== query table===\n")
//    session.sql(
//      """
//        |select b.title, b.year_published, a.first_name || ' '||a.last_name AS NAME from BOOK b
//        |JOIN BOOK_TO_AUTHOR ba on ba.book_uid = b.book_uid
//        |JOIN AUTHOR a ON ba.author_uid = a.author_uid
//        |GROUP BY  b.title, b.year_published, a.last_name, a.first_name""".stripMargin).show()
  }
}

