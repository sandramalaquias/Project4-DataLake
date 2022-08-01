import configparser, argparse

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col, trim, count
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime, to_timestamp, expr, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType,DoubleType,LongType


#---------------------Get Input/output/config path -----------------------------------
    
""" This function get input/output/config path as param from spark-submit
    All arguments are optional
    inputpath  => standard path for read data
    outputpath => standard path to write data
    configpath => path for get credentials. This is ".cfg" type
    localRun    = Y/N => param to spark
    
    If input/output/config/local is not informed, will working using
        Input_path = 's3a://udacity-dend/"
        Output_path = 's3a://smmbucketudacity" (this is my bucket)
        Configpath  = has no defaul value
        localRun    = "n"
        
    Examples:
        spark-submit spark_docker.py --inputpath s3a://udacity-dend --outputpath s3a://smmbucketudacity --configpath dl.cfg 
                                     --localrun y

        To get help: spark-submit spark_docker.py --help
    
    Return input_path, output_path, config_path, localrun
"""

def get_path():
    # Initiate the parser
    parser = argparse.ArgumentParser(description='Path for input/output/config files')

    # Add long and short argument
    parser.add_argument("--inputpath", "-inputpath", help="The URI for Udacity bucket location like s3a://udacity-dend")
    parser.add_argument("--outputpath", "-outputpath", help="The URI for your S3 bucket location like s3a://smmbucketudacity")
    parser.add_argument("--configpath", "-configtpath", help="The URI for your AWS config file like /home/data/dl.cfg")
    parser.add_argument("--localrun", "-localrun", help="N=default (NO), Y=run in local mode (yes)")
    
    # Read arguments from the command line
    args = parser.parse_args()

    # Check for --inputpath
    if args.inputpath:
        inputpath=args.inputpath
    else:
        inputpath = 's3a://udacity-dend/'
        
    # Check for --outputpath
    if args.outputpath:
        outputpath = args.outputpath
    else:
        outputpath ='s3a://smmbucketudacity/'
        
    if not args.localrun or (args.localrun.lower() != 'y' and 'n'):
        localrun = 'n'
    else:
        localrun = args.localrun.lower()
        
        
    return inputpath,\
            outputpath,\
            localrun,\
           '' if args.configpath is None else args.configpath     
  


#-----------------GET AWS Credentials  ------------------------------------------------
    
""" This function get AWS credential from configpath 
    If the configpath is unacessable, return empty values
    
    Input:  config_path
    Output: Return AWS Credentials (ACCESS, SECRET, TOKEN) or empty values
    
"""

def get_credentials(config_path):
    config = configparser.ConfigParser()

    try:
        with open(config_path) as credentials:
            config.read_file(credentials)
            return config['AWS']['AWS_ACCESS_KEY_ID'],\
                   config['AWS']['AWS_SECRET_ACCESS_KEY'],\
                   config['AWS']['AWS_SESSION_TOKEN']     

    except IOError:
        return "", "", ""


#-----------------Working with Spark Session-------------------------------------------------------

""" The function "create_spark_session" creates a spark session using AWS credentials if config file was informed
    In sparkConf, set enviroment to access AWS S3 using TemporaryCredentials if TOKEN Credential is provided
    To read s3a, neeed to enable S3V4 to JavaOptions
    In this case was needed to use: SparkConf, sparkContex and SparkSession to flag all AWS enviroment
    
    Input: aws credencials (access_key, secret_key, token)
           localrun => identify the type of run 
           
    Output: Return spark session as spark
"""
    
def create_spark_session(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_TOKEN, localrun):
    conf = SparkConf()
    conf.setAppName('pyspark_aws')
    conf.set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
    conf.set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
    
    ## configure AWS Credentials
    if AWS_TOKEN:
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
        conf.set('spark.hadoop.fs.s3a.session.token', AWS_TOKEN)
     
    if AWS_ACCESS_KEY:        
        conf.set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        
    if localrun == 'y':
        conf.setMaster('local[*]')
        
    if AWS_SECRET_KEY:    
        conf.set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
    
    print(conf.toDebugString())

    sc = SparkContext(conf=conf)
    sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

    spark = SparkSession \
            .builder \
            .appName('pyspark_aws') \
            .getOrCreate()

    print('session created')

    return spark 


#---------------WORKING WITH SONG DATA --------------------------------------
"""
The function "process_song_data" works with Song Data
    Read the file from S3 bucket
    Use recurssiveFileLookup => read all the files in the path
    Transform in DataFrame and Temporay View
    Select columns to format tables song and artist
    Save then on my S3 bucket using parquet file type

About Malformad data
    Have three ways to handle this type of data
    - To include this data in a separate column (mode = PERMISSIVE)
    - To ignore all bad records (mode = DROPMALFORMED)
    - Throws an exception when it meets corrupted records (mode = FAILFAST)
     Assuming that completely broken records will not part of out dataset
     
Input: spark context
       input_path  => partial path for song_data
       output_path => partial path to write tables
       number => 0 = all path for input data |  1 = partial path for input data

Output: songs_table 
        artists_table
    
"""
    
def process_song_data(spark, input_data, output_data, number):

# get filepath to song data file
# Number = 1 => get partial data path
# Number = 0 => get total data path

    if  number == 1:
        song_data_path = input_data + '/song-data/A/A/*/*.json'       
    else:
        song_data_path = input_data + '/song_data/*/*/*/*.json'
        
    print(song_data_path)
    
## defining schema to read the path
    song_schema=StructType([
        StructField("artist_id", StringType(),True),
        StructField("artist_latitude",StringType(), True),
        StructField("artist_location",StringType(), True),
        StructField("artist_longitude",StringType(), True),
        StructField("artist_name",StringType(),True),
        StructField("duration",DoubleType(),True),
        StructField("num_songs",LongType(),True),
        StructField("song_id",StringType(),True),
        StructField("title",StringType(),True),
        StructField("year",LongType(),True)
        ])

## read song_data using pre-defined schema
    df = spark.read.schema(song_schema)\
        .option("mode", "DROPMALFORMED")\
        .option("recursiveFileLookup", "true")\
        .json(song_data_path)
                             
    df.createOrReplaceTempView("song_data")

# songs_table columns: song_id, title, artist_id, year, duration
# extract columns to create songs table
# remove white spaces from right/left "title" column

    songs_table = spark.sql\
    ("""
        select distinct song_id, trim(title) as title, artist_id, year, duration 
           from     song_data
           where   song_id is not null   and
                   title is not null     and
                   artist_id is not null
           order by year, artist_id
    """)
    

## artist table columns: artist_id, name, location, latitude, longitude
# extract columns to create artists table
# # remove white spaces from right/left "artist_name" column

    artists_table = spark.sql\
    ("""
                select distinct artist_id, trim(artist_name) as name, artist_location as location, 
                       Double(artist_latitude) as latitude, Double(artist_longitude) as longitude
                from   song_data
                where  artist_id is not null and
                       artist_name is not null
                order by artist_id
    """) 


# write songs table to parquet files partitioned by year and artist
# defining schema to songstable: song_id, trim(title) as title, artist_id, year, duration 
    
    output_path = output_data + '/songs_table'

    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_path)
    
    print ('Finish song_table')

    
# write artists table to parquet files
    output_path = output_data + '/artists_table'
    artists_table.write.partitionBy("artist_id").mode("overwrite").parquet(output_path)
    
    print ('Finish artist_table')
    
    
    return songs_table, artists_table


    
#-----------------WORKING WITH LOG DATA----------------------------------------------------
"""
The function "process_log_data" works with Log Data
    Read Log the file from S3 bucket (udacity - Log input data)
    Get Song and Artists from process song data
    Use recurssiveFileLookup => read files in the path
    Transform in DataFrame and Temporay View
    Select and create columns to format tables: time, users and songplay
    Save it on S3 in parquet files

About Malformad data
    Have three ways to handle this type of data
    - To include this data in a separate column (mode = PERMISSIVE)
    - To ignore all bad records (mode = DROPMALFORMED)
    - Throws an exception when it meets corrupted records (mode = FAILFAST)
     Assuming that completely broken records will not part of out dataset

About Broadcasting decicions 
   1.there are less artists in log_data than artists_table
   2.there are less songs in log_data than songs_table
   
   Spark will share small table a.k.a broadcast table to all data nodes where big table data is present. 
   In this case, we need all the data from small table but only matching data from big table. 
   So spark doesn't know if this record was matched at another data node or even there was no match at all. 
   Due to this ambiguity it cannot select all the records from small table(if this was distributed). 
   So spark is not using Broadcast Join in this case.
   
   Conclusion: songs and artists tables will not be broadcasted to better join

Inputs: spark = sparkContext
        input_data = path for songs
        output_data = path for write tables
        number => 0 = all path for input data |  1 = partial path for input data
        songs_table => get from process_song_data function to avoid read them again
        artists_table => get from process_song_data function to avoid read them again
   
Output: No return

    
"""
def process_log_data(spark, input_data, output_data, number, songs_table, artists_table):
# get filepath to log data file
    if  number == 1:
        log_data_path = input_data + '/log-data/2018/11/*.json' 
    else:
        log_data_path = input_data + '/log-data/*/*/*.json'
        
    print (log_data_path)        

# defining schema to read the path
    log_schema=StructType([
         StructField("artist", StringType(),True),
         StructField("auth", StringType(),True),
         StructField("firstName", StringType(),True),
         StructField("gender", StringType(),True),
         StructField("itemInSession", LongType(),True),
         StructField("lastName", StringType(),True),
         StructField("length", DoubleType(),True),
         StructField("level", StringType(),True),
         StructField("location", StringType(),True),
         StructField("method", StringType(),True),
         StructField("page", StringType(),True),
         StructField("registration", DoubleType(),True),
         StructField("sessionId", LongType(),True),
         StructField("song", StringType(),True),
         StructField("status", LongType(),True),
         StructField("ts", LongType(),True),
         StructField("userAgent", StringType(),True),
         StructField("userId", StringType(),True)
    ])

    
# read log data file
    df = spark.read.schema(log_schema)\
                .option("recursiveFileLookup", "true")\
                .option("mode", "DROPMALFORMED")\
                .json(log_data_path)                               


## transform TS from second to datetime and create a column songlay_id
    df_log = df.filter(df.page == "NextSong")\
            .withColumn('start_time', to_timestamp(expr("from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss')")))\
            .dropDuplicates()\
            .na.drop("all")
        
         
## create temporary view
    df_log.createOrReplaceTempView("log_data")


#time - timestamps of records in songplays broken down into specific units

    time_table = spark.sql\
    ("""
        select distinct start_time, 
               hour(start_time) as hour, 
               day(start_time) as day,
               weekofyear(start_time) as week,
               month(start_time) as month,
               year(start_time) as year,
               dayofweek(start_time) as weekday,
               date_format(start_time, "E") as dayname
       from log_data
        where ts is not null
    """)


## extract columns for users table  
## user_id, first_name, last_name, gender, level
## using distinc userId the empty value is not selected

    users_table = spark.sql\
    ("""
        select distinct userId as user_id, firstName as first_name, lastName as last_name, gender, level
        from log_data
        where userId is not null
    """)


## extract columns for songsplay  
## using row_number to automatically fill the songsplay_id

    songs_table.createOrReplaceTempView("songs")
    artists_table.createOrReplaceTempView("artists")
    
    songplay = spark.sql("""
    select log_data.start_time, log_data.userId, log_data.level, 
           songs.song_id, songs.artist_id,  
           log_data.sessionId, log_data.location, log_data.userAgent,
           row_number() over (order by log_data.start_time) as songplay_id
    from   log_data
    left join artists on log_data.artist == artists.name
    left join songs   on log_data.song == songs.title and artists.artist_id == songs.artist_id
    order by log_data.start_time
    """)
    
    songplay.dropDuplicates().na.drop("all")   
    songplay.show()

# write time table to parquet files
    output_path = output_data + '/time_table'
    time_table.write.partitionBy("year").mode("overwrite").parquet(output_path)
    print ('Finish time_table')
    
# write users table to parquet files
    output_path = output_data + '/users_table'
    users_table.write.partitionBy("user_id").mode("overwrite").parquet(output_path)
    print ('Finish users_table')
    

# write songplay table to parquet files
    output_path = output_data + '/songsplay_table'
    songplay.write.partitionBy("user_id").mode("overwrite").parquet(output_path)
    print ('Finish songsplay_table')
    
#get tables lengths
    
    songs_len = songs_table.count()
    artists_len = artists_table.count()
    users_len = users_table.count()
    time_len = time_table.count()
    songplay_len = songplay.count()
    
    print ('============> Qtd of: songs {}, artists {}, users {}, time {}, songsplay {}'.\
           format(songs_len, artists_len, users_len, time_len, songplay_len))

#---------------------------------------

def main():
    print ("Start of process")

    #path of input/output/config data
    input_data, output_data, localmode, config_data = get_path()
    print (input_data, output_data, localmode, config_data)
    
    #get AWS credentials
    if config_data:    
        AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_TOKEN = get_credentials(config_data)
    else:
        AWS_ACCESS_KEY = ""
        AWS_SECRET_KEY = ""
        AWS_TOKEN      = ""
    
    #start spark
    spark = create_spark_session(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_TOKEN, localmode)
    
    #get path of files to be processed (0 = all / 1=partial one)
    number = 0
    
    ##process udacity-dend song-data
    songs_table, artists_table = process_song_data(spark, input_data, output_data, number)   
    
    ##process udacity-dend log-data
    process_log_data(spark, input_data, output_data, number, songs_table, artists_table)   
    
    spark.stop()
    
    print ("End of process")


if __name__ == "__main__":
    main()