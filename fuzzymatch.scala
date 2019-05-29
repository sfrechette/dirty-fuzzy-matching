
import org.apache.spark.sql.SQLContext
import sqlContext.implicits._

val sqlContext = new SQLContext(sc)

// Soundex algorithm - encoding
val csvfile = "/Users/stephanefrechette/Projects/dirty-fuzzy-matching/dataset01.csv"
val df_soundex = sqlContext.read.option("header", "true").option("inferSchema", "true").csv(csvfile)

val soundexDF = df_soundex.withColumn (
  "col1_soundex",
  soundex(col("col1"))
).withColumn(
  "col2_soundex",
  soundex(col("col2"))
)
soundexDF.show()

// Soundex algorithm - phonetically equal
val csvfile = "/Users/stephanefrechette/Projects/dirty-fuzzy-matching/dataset01.csv"
val df_phonetic = sqlContext.read.option("header", "true").option("inferSchema", "true").csv(csvfile)

val phoneticDF = df_phonetic.withColumn(
  "col1_col2_soundex_equality",
  soundex(col("col1")) === soundex(col("col2"))
)
phoneticDF.show()

// Levenshtein Distance - between words
val csvfile = "/Users/stephanefrechette/Projects/dirty-fuzzy-matching/dataset01.csv"
val df_levenshtein = sqlContext.read.option("header", "true").option("inferSchema", "true").csv(csvfile)

val levenshteinDF = df_levenshtein.withColumn(
  "col1_col2_levenshtein",
  levenshtein(col("col1"), col("col2"))
)
levenshteinDF.show()