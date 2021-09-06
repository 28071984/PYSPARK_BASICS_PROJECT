import pyspark
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.Builder().master("local[1]").appName("test").getOrCreate()
name_basics_tsv_file = "C:/Users/nageg/Downloads/name.basics.tsv/data.tsv"
name_basics = spark.read.format("csv").options(header='True', delimiter="\t").load(name_basics_tsv_file)
name_basics.show()

title_akas_tsv_file = "C:/Users/nageg/Downloads/title.akas.tsv/data.tsv"
title_akas = spark.read.format("csv").options(header='True', delimiter="\t").load(title_akas_tsv_file)
title_akas.show()

title_basics_tsv_file = "C:/Users/nageg/Downloads/title.basics.tsv/data.tsv"
title_basics = spark.read.format("csv").options(header='True', delimiter="\t").load(title_basics_tsv_file)
title_basics.show()


title_crew_tsv_file = "C:/Users/nageg/Downloads/title.crew.tsv/data.tsv"
title_crew = spark.read.format("csv").options(header='True', delimiter="\t").load(title_crew_tsv_file)
title_crew.show()

title_ratings_tsv_file = "C:/Users/nageg/Downloads/title.ratings.tsv/data.tsv"
title_ratings = spark.read.format("csv").options(header='True', delimiter="\t").load(title_ratings_tsv_file)
title_ratings.show()

title_episode_tsv_file = "C:/Users/nageg/Downloads/title.episode.tsv/data.tsv"
title_episode = spark.read.format("csv").options(header='True', delimiter="\t").load(title_episode_tsv_file)
title_episode.show()

title_principals_tsv_file = "C:/Users/nageg/Downloads/title.principals.tsv/data.tsv"
title_principals = spark.read.format("csv").options(header='True', delimiter="\t").load(title_principals_tsv_file)
title_principals.show()

'''Get the list of people’s names, who were born in the 19th century.'''

actors19 = name_basics.filter((F.col("birthYear") < 1900) & (F.col("birthYear") > 1799)).select(F.col("primaryName"),F.col("birthYear"))
print("Actors count who was born in  19th century: " + str(actors19.count()))
actors19.show()
write_pdf = actors19.toPandas()
write_pdf.to_csv("C:/Outbound/actors19.csv")

'''Get all titles of series/movies etc. that are available in Ukrainian.'''

UA_language = title_akas.filter((F.col("region") == 'UA')&(F.col("language")!='\\N')&(F.col("language")!='en')).select(F.col("region"),F.col("language"),F.col("titleId"))
UA_language = UA_language.join(title_basics.select(F.col("primaryTitle"),F.col("tconst")),title_basics.tconst==UA_language.titleId,"inner")
UA_language = UA_language.drop(F.col("tconst")).drop(F.col("titleId"))
print("Ukrainian movies: " + str(UA_language.count()))
UA_language.show()
UA_language.repartition(1).write.format("csv").mode("overwrite").option("header","true").options(encodings = 'UTF-8').save("C:/Outbound/ua_language.csv")



'''Get names of people, corresponding movies/series and characters they played in those films.'''

Actors = title_principals.filter(F.col("category") == 'actor').select(F.col("category"),F.col("characters"),F.col("tconst"),F.col("nconst"))
Actors = Actors.join(name_basics.select(F.col("nconst"),F.col("primaryName")),name_basics.nconst==Actors.nconst,"inner")
Actors = Actors.join(title_akas.select(F.col("titleId"),F.col("title")),Actors.tconst==title_akas.titleId,"inner")\
    .select(F.col("title"),F.col("characters"),F.col("primaryName")).distinct().withColumnRenamed("title","movie_title")\
    .withColumnRenamed("primaryName","actor_name")
print("Actors: " + str(Actors.count()))
Actors.show()
Actors.repartition(1).write.format("csv").mode("overwrite").option("header","true").save("C:/Outbound/actors_movie_character.csv")



'''Get titles of all movies that last more than 2 hours.'''


more2hours = title_basics.filter(F.col("runtimeMinutes") > 120).select(F.col("primaryTitle"),F.col("runtimeMinutes"))
print("Movies longer than 2 hours: " + str(more2hours.count()))
more2hours.show()
write_pdf = more2hours.toPandas()
write_pdf.to_csv("C:/Outbound/more2hours.csv")



'''Get information about how many episodes in each TV Series. Get the top 50 of them starting from the TV Series with the biggest quantity of episodes.'''


Episodes=title_episode.filter(F.col("episodeNumber")>0).select(F.col("tconst"), F.col("episodeNumber"))
Episodes=Episodes.join(title_basics.select(F.col("tconst"),F.col("titleType"),F.col("primaryTitle")), Episodes.tconst==title_basics.tconst,"inner")\
    .orderBy(F.col("episodeNumber"), ascending=False).limit(50).select(F.col("primaryTitle"), F.col("titleType"), F.col("episodeNumber"))
Episodes.show()
write_pdf = Episodes.toPandas()
write_pdf.to_csv("C:/Outbound/50topEpisodes.csv")


'''Get 10 titles of the most popular movies/series etc. by each decade.'''

MoviesDecades = title_ratings.join(title_basics.filter(F.col("startYear") != '\\N').select(F.col("tconst"), F.col("primaryTitle"),F.col("startYear"), F.col("genres")), title_ratings.tconst==title_basics.tconst,"inner")
window = Window.partitionBy(F.col("decade")).orderBy(F.col("averageRating").desc(),F.col("numVotes").asc_nulls_last())
df = MoviesDecades.drop("tconst").withColumn('decade',F.floor(F.col("startYear")/10))
ordered_df = df.select(df.columns + [F.rank().over(window).alias('row_rank')])
df = ordered_df.filter(f"row_rank <= 10").drop("row_rank").orderBy(F.col("decade").asc_nulls_last(),F.col("averageRating").desc(),F.col("numVotes").desc())
MoviesDecades  = df.select(["averageRating","primaryTitle","startYear"])
MoviesDecades.show(100)
write_pdf = MoviesDecades.toPandas()
write_pdf.to_csv("C:/Outbound/MoviesDecades.csv")


'''Get 10 titles of the most popular movies/series etc. by each genre.'''

MoviesGenres = title_ratings.join(title_basics.select(F.col("tconst"), F.col("primaryTitle"), F.col("genres")), title_ratings.tconst==title_basics.tconst,"inner")
window = Window.partitionBy(F.col("genres")).orderBy(F.col("averageRating").desc(),F.col("numVotes").asc_nulls_last())
df = MoviesGenres.drop("tconst")
ordered_df = df.select(df.columns + [F.rank().over(window).alias('row_rank')])
df = ordered_df.filter(f"row_rank <= 10").drop("row_rank")
MoviesGenres = df.select(["genres","averageRating","primaryTitle"]).orderBy(F.col("genres"),F.col("averageRating").desc())
MoviesGenres.show(100)
write_pdf = MoviesGenres.toPandas()
write_pdf.to_csv("C:/Outbound/MoviesGenres.csv")

'''Get information about how many adult movies/series etc. there are per region. Get the top 100 of them from the region with the biggest count to the region with the smallest one.'''
AdultMovies=title_basics.filter(F.col("isAdult")==1).select(F.col("tconst"))
AdultMovies=AdultMovies.join(title_akas.select(F.col("region"),F.col("titleId")), AdultMovies.tconst==title_akas.titleId, "inner")
AdultMovies=AdultMovies.groupby(F.col("region")).count()
AdultMovies=AdultMovies.orderBy(F.col("count").desc()).limit(100)
AdultMovies.show(100)
write_pdf = AdultMovies.toPandas()
write_pdf.to_csv("C:/Outbound/AdultMovies.csv")