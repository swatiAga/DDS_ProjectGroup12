import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

s = SparkSession.builder.getOrCreate()

def read_file(df, path):
    df = s.read.option("multiline", "true").json(path)
    df.createOrReplaceTempView('Movies')
    return df

def top_ten_most_popular(df):
    return df.select("title", "popularity").orderBy("popularity", ascending=False).limit(10)

def yearly_stats(df):
    movies_with_year = df.withColumn("release_year", year("release_date"))
    yearly_stats = movies_with_year.groupBy("release_year").agg(avg("popularity").alias("avg_popularity"),
                                                                avg("vote_average").alias("avg_rating"))
    return yearly_stats.orderBy("release_year", ascending = False).limit(10)

def most_movies(df):
    movies_with_year = df.withColumn("release_year", year("release_date"))
    movies_per_year = movies_with_year.groupBy("release_year").count()
    most_movies_year = movies_per_year.orderBy("count", ascending = False)
    return most_movies_year.limit(1)

def most_popular_per_year(df):
    movies_with_year = df.withColumn("release_year", year("release_date"))
    year_window = Window.partitionBy("release_year").orderBy(df['popularity'].desc())
    ranked_movies = movies_with_year.withColumn("rank", row_number().over(year_window))
    most_popular_per_year = ranked_movies.filter("rank = 1")
    return most_popular_per_year.select("release_year", "title", "popularity").orderBy("release_year", ascending=False).limit(10)

def top_franchises(df):
    extract_franchise = udf(lambda title: title.split(":")[0] if ":" in title else None, StringType())
    franchises = df.withColumn("franchise", extract_franchise("title"))
    franchises = franchises.filter(franchises['franchise'].isNotNull())
    stats = franchises.groupBy("franchise").agg(
        count("*").alias("num_movies"),
        avg("vote_average").alias("avg_rating"),
        avg("popularity").alias("avg_popularity")
    )
    top_franchises = stats.orderBy("num_movies", ascending = False)
    return top_franchises.limit(10)


