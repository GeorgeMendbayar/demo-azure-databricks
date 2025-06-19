# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = spark.sql("describe external location `data_zone`").select("url").collect()[0][0]
        self.base_dir_checkpoint = spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.db_name = "sbit_db"
        self.maxFilesPerTrigger = 1000

# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class SetupHelper():   
    def __init__(self, env):
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/raw"
        self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"        
        self.catalog = env
        self.db_name = Conf.db_name
        self.initialized = False
        
    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Done")
        
    def create_registered_users_bz(self):
        if(self.initialized):
            print(f"Creating registered_users_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
                    user_id long,
                    device_id long, 
                    mac_address string, 
                    registration_timestamp double,
                    load_time timestamp,
                    source_file string                    
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    
    def create_gym_logins_bz(self):
        if(self.initialized):
            print(f"Creating gym_logins_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
                    mac_address string,
                    gym bigint,
                    login double,                      
                    logout double,                    
                    load_time timestamp,
                    source_file string
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_kafka_multiplex_bz(self):
        if(self.initialized):
            print(f"Creating kafka_multiplex_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                  key string, 
                  value string, 
                  topic string, 
                  partition bigint, 
                  offset bigint, 
                  timestamp bigint,                  
                  date date, 
                  week_part string,                  
                  load_time timestamp,
                  source_file string)
                  PARTITIONED BY (topic, week_part)
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")       
    
            
    def create_users(self):
        if(self.initialized):
            print(f"Creating users table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                    user_id bigint, 
                    device_id bigint, 
                    mac_address string,
                    registration_timestamp timestamp
                    )
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")            
    
    def create_gym_logs(self):
        if(self.initialized):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                    mac_address string,
                    gym bigint,
                    login timestamp,                      
                    logout timestamp
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_user_profile(self):
        if(self.initialized):
            print(f"Creating user_profile table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                    user_id bigint, 
                    dob DATE, 
                    sex STRING, 
                    gender STRING, 
                    first_name STRING, 
                    last_name STRING, 
                    street_address STRING, 
                    city STRING, 
                    state STRING, 
                    zip INT, 
                    updated TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_heart_rate(self):
        if(self.initialized):
            print(f"Creating heart_rate table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                    device_id LONG, 
                    time TIMESTAMP, 
                    heartrate DOUBLE, 
                    valid BOOLEAN)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

            
    def create_user_bins(self):
        if(self.initialized):
            print(f"Creating user_bins table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workouts(self):
        if(self.initialized):
            print(f"Creating workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                    user_id INT, 
                    workout_id INT, 
                    time TIMESTAMP, 
                    action STRING, 
                    session_id INT)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_completed_workouts(self):
        if(self.initialized):
            print(f"Creating completed_workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT, 
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workout_bpm(self):
        if(self.initialized):
            print(f"Creating workout_bpm table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT,
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP,
                    time TIMESTAMP, 
                    heartrate DOUBLE)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_date_lookup(self):
        if(self.initialized):
            print(f"Creating date_lookup table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
                    date date, 
                    week int, 
                    year int, 
                    month int, 
                    dayofweek int, 
                    dayofmonth int, 
                    dayofyear int, 
                    week_part string)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_workout_bpm_summary(self):
        if(self.initialized):
            print(f"Creating workout_bpm_summary table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                    workout_id INT, 
                    session_id INT, 
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING, 
                    min_bpm DOUBLE, 
                    avg_bpm DOUBLE, 
                    max_bpm DOUBLE, 
                    num_recordings BIGINT)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_gym_summary(self):
        if(self.initialized):
            print(f"Creating gym_summar gold view...", end='')
            spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                            SELECT to_date(login::timestamp) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
                        """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db()       
        self.create_registered_users_bz()
        self.create_gym_logins_bz() 
        self.create_kafka_multiplex_bz()        
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()  
        self.create_gym_summary()
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("registered_users_bz")   
        self.assert_table("gym_logins_bz")        
        self.assert_table("kafka_multiplex_bz")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("user_bins")
        self.assert_table("date_lookup")
        self.assert_table("workout_bpm_summary") 
        self.assert_table("gym_summary") 
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)
        print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done")    

# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class HistoryLoader():
    def __init__(self, env):
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/raw"      
        self.test_data_dir = Conf.base_dir_data + "/test_data"
        self.catalog = env
        self.db_name = Conf.db_name
        
    def load_date_lookup(self):        
        print(f"Loading date_lookup table...", end='')        
        spark.sql(f"""INSERT OVERWRITE TABLE {self.catalog}.{self.db_name}.date_lookup 
                SELECT date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part 
                FROM json.`{self.test_data_dir}/6-date-lookup.json/`""")
        print("Done")
        
    def load_history(self):
        import time
        start = int(time.time())
        print(f"\nStarting historical data load ...")
        self.load_date_lookup()
        print(f"Historical data load completed in {int(time.time()) - start} seconds")
        
    def assert_count(self, table_name, expected_count):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")        
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting historical data load validation...")
        self.assert_count(f"date_lookup", 365)
        print(f"Historical data load validation completed in {int(time.time()) - start} seconds")               



# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Bronze():
    def __init__(self, env):        
        self.Conf = Config()
        self.landing_zone = self.Conf.base_dir_data + "/raw" 
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def consume_user_registration(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "user_id long, device_id long, mac_address string, registration_timestamp double"
        
        df_stream = (spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", 1)
                        .option("cloudFiles.format", "csv")
                        .option("header", "true")
                        .load(self.landing_zone + "/registered_users_bz")
                        .withColumn("load_time", F.current_timestamp()) 
                        .withColumn("source_file", F.input_file_name())
                    )
                        
        # Use append mode because bronze layer is expected to insert only from source
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/registered_users_bz") \
                                 .outputMode("append") \
                                 .queryName("registered_users_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
          
    def consume_gym_logins(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "mac_address string, gym bigint, login double, logout double"
        
        df_stream = (spark.readStream 
                        .format("cloudFiles") 
                        .schema(schema) 
                        .option("maxFilesPerTrigger", 1) 
                        .option("cloudFiles.format", "csv") 
                        .option("header", "true") 
                        .load(self.landing_zone + "/gym_logins_bz") 
                        .withColumn("load_time", F.current_timestamp())
                        .withColumn("source_file", F.input_file_name())
                    )
        
        # Use append mode because bronze layer is expected to insert only from source
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/gym_logins_bz") \
                                 .outputMode("append") \
                                 .queryName("gym_logins_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
            
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        
        
    def consume_kafka_multiplex(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "key string, value string, topic string, partition bigint, offset bigint, timestamp bigint"
        df_date_lookup = spark.table(f"{self.catalog}.{self.db_name}.date_lookup").select("date", "week_part")
        
        df_stream = (spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", 1)
                        .option("cloudFiles.format", "json")
                        .load(self.landing_zone + "/kafka_multiplex_bz")                        
                        .withColumn("load_time", F.current_timestamp())       
                        .withColumn("source_file", F.input_file_name())
                        .join(F.broadcast(df_date_lookup), 
                              [F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date")], 
                              "left")
                    )
        
        # Use append mode because bronze layer is expected to insert only from source
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/kafka_multiplex_bz") \
                                 .outputMode("append") \
                                 .queryName("kafka_multiplex_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p1")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
        
            
    def consume(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nStarting bronze layer consumption ...")
        self.consume_user_registration(once, processing_time) 
        self.consume_gym_logins(once, processing_time) 
        self.consume_kafka_multiplex(once, processing_time)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating bronz layer records...")
        self.assert_count("registered_users_bz", 5 if sets == 1 else 10)
        self.assert_count("gym_logins_bz", 8 if sets == 1 else 16)
        self.assert_count("kafka_multiplex_bz", 7 if sets == 1 else 13, "topic='user_info'")
        self.assert_count("kafka_multiplex_bz", 16 if sets == 1 else 32, "topic='workout'")
        self.assert_count("kafka_multiplex_bz", sets * 253801, "topic='bpm'")
        print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")                


# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class CDCUpserter:
    def __init__(self, merge_query, temp_view_name, id_column, sort_by):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        self.id_column = id_column
        self.sort_by = sort_by 
        
    def upsert(self, df_micro_batch, batch_id):
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F
        
        window = Window.partitionBy(self.id_column).orderBy(F.col(self.sort_by).desc())
        
        df_micro_batch.filter(F.col("update_type").isin(["new", "update"])) \
                .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank") \
                .createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class Silver():
    def __init__(self, env):
        self.Conf = Config() 
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def upsert_users(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        #Idempotent - User cannot register again so ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.users a
            USING users_delta b
            ON a.user_id=b.user_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "users_delta")
        
        # Spark Structured Streaming accepts append only sources. 
        #      - This is not a problem for silver layer streams because bronze layer is insert only
        #      - However, you may want to allow bronze layer deletes due to regulatory compliance 
        # Spark Structured Streaming throws an exception if any modifications occur on the table being used as a source
        #      - This is a problem for silver layer streaming jobs.
        #      - ignoreDeletes allows to delete records on partition column in the bronze layer without exception on silver layer streams 
        # Starting version is to allow you to restart your stream from a given version just in case you need it
        #      - startingVersion is only applied for an empty checkpoint
        # Limiting your input stream size is critical for running on limited capacity
        #      - maxFilesPerTrigger/maxBytesPerTrigger can be used with the readStream
        #      - Default value is 1000 for maxFilesPerTrigger and maxBytesPerTrigger has no default value
        #      - The recomended maxFilesPerTrigger is equal to #executors assuming auto optimize file size to 128 MB
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.registered_users_bz")
                         .selectExpr("user_id", "device_id", "mac_address", "cast(registration_timestamp as timestamp)")
                         .withWatermark("registration_timestamp", "30 seconds")
                         .dropDuplicates(["user_id", "device_id"])
                   )
        
        # We read new records in bronze layer and insert them to silver. So, the silver layer is insert only in a typical case. 
        # However, we want to ignoreDeletes, remove duplicates and also merge with an update statement depending upon the scenarios
        # Hence, it is recomended to se the update mode
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/users")
                                 .queryName("users_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
          
    def upsert_gym_logs(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        #Idempotent - Insert new login records 
        #           - Update logout time when 
        #                   1. It is greater than login time
        #                   2. It is greater than earlier logout
        #                   3. It is not NULL (This is also satisfied by above conditions)
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.gym_logs a
            USING gym_logs_delta b
            ON a.mac_address=b.mac_address AND a.gym=b.gym AND a.login=b.login
            WHEN MATCHED AND b.logout > a.login AND b.logout > a.logout
              THEN UPDATE SET logout = b.logout
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "gym_logs_delta")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.gym_logins_bz")
                         .selectExpr("mac_address", "gym", "cast(login as timestamp)", "cast(logout as timestamp)")
                         .withWatermark("login", "30 seconds")
                         .dropDuplicates(["mac_address", "gym", "login"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/gym_logs")
                                 .queryName("gym_logs_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
    
    def upsert_user_profile(self, once=False, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F

        #Idempotent - Insert new record
        #           - Ignore deletes
        #           - Update user details when
        #               1. update_type in ("new", "append")
        #               2. current update is newer than the earlier
        schema = """
            user_id bigint, update_type STRING, timestamp FLOAT, 
            dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, 
            address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>
            """
        
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.user_profile a
            USING user_profile_cdc b
            ON a.user_id=b.user_id
            WHEN MATCHED AND a.updated < b.updated
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
            """
        
        data_upserter = CDCUpserter(query, "user_profile_cdc", "user_id", "updated")
        
        df_cdc = (spark.readStream
                       .option("startingVersion", startingVersion)
                       .option("ignoreDeletes", True)
                       #.option("withEventTimeOrder", "true")
                       #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                       .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
                       .filter("topic = 'user_info'")
                       .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                       .select("v.*")
                       .select("user_id", F.to_date('dob','MM/dd/yyyy').alias('dob'),
                               'sex', 'gender','first_name','last_name', 'address.*',
                               F.col('timestamp').cast("timestamp").alias("updated"),
                               "update_type")
                       .withWatermark("updated", "30 seconds")
                       .dropDuplicates(["user_id", "updated"])
                 )
    
        stream_writer = (df_cdc.writeStream
                               .foreachBatch(data_upserter.upsert) 
                               .outputMode("update") 
                               .option("checkpointLocation", f"{self.checkpoint_base}/user_profile") 
                               .queryName("user_profile_stream")
                        )
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
    
    def upsert_workouts(self, once=False, processing_time="10 seconds", startingVersion=0):
        from pyspark.sql import functions as F        
        schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"
        
        #Idempotent - User cannot have two workout sessions at the same time. So ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.workouts a
            USING workouts_delta b
            ON a.user_id=b.user_id AND a.time=b.time
            WHEN NOT MATCHED THEN INSERT *
            """

        data_upserter=Upserter(query, "workouts_delta")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
                         .filter("topic = 'workout'")
                         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                         .select("v.*")
                         .select("user_id", "workout_id", 
                                 F.col("timestamp").cast("timestamp").alias("time"), 
                                 "action", "session_id")
                         .withWatermark("time", "30 seconds")
                         .dropDuplicates(["user_id", "time"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation",f"{self.checkpoint_base}/workouts")
                                 .queryName("workouts_upsert_stream")
                        )
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
        
    def upsert_heart_rate(self, once=False, processing_time="10 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"
        
        #Idempotent - Only one BPM signal is allowed at a timestamp. So ignore the duplicates and insert the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.heart_rate a
        USING heart_rate_delta b
        ON a.device_id=b.device_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "heart_rate_delta")
    
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
                         .filter("topic = 'bpm'")
                         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                         .select("v.*", F.when(F.col("v.heartrate") <= 0, False).otherwise(True).alias("valid"))
                         .withWatermark("time", "30 seconds")
                         .dropDuplicates(["device_id", "time"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/heart_rate")
                                 .queryName("heart_rate_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
          
    
    def age_bins(self, dob_col):
        from pyspark.sql import functions as F
        age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
        return (F.when((age_col < 18), "under 18")
                .when((age_col >= 18) & (age_col < 25), "18-25")
                .when((age_col >= 25) & (age_col < 35), "25-35")
                .when((age_col >= 35) & (age_col < 45), "35-45")
                .when((age_col >= 45) & (age_col < 55), "45-55")
                .when((age_col >= 55) & (age_col < 65), "55-65")
                .when((age_col >= 65) & (age_col < 75), "65-75")
                .when((age_col >= 75) & (age_col < 85), "75-85")
                .when((age_col >= 85) & (age_col < 95), "85-95")
                .when((age_col >= 95), "95+")
                .otherwise("invalid age").alias("age"))
        
    
    def upsert_user_bins(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        # Idempotent - This table is maintained as SCD Type 1 dimension
        #            - Insert new user_id records 
        #            - Update old records using the user_id

        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.user_bins a
            USING user_bins_delta b
            ON a.user_id=b.user_id
            WHEN MATCHED 
              THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "user_bins_delta")
        
        df_user = spark.table(f"{self.catalog}.{self.db_name}.users").select("user_id")
        
        # Running stream on silver table requires ignoreChanges
        # No watermark required - Stream to staic join is stateless
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreChanges", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.user_profile")
                         .join(df_user, ["user_id"], "left")
                         .select("user_id", self.age_bins(F.col("dob")),"gender", "city", "state")
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/user_bins")
                                 .queryName("user_bins_upsert_stream")
                        )
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")

        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
                  
            
    def upsert_completed_workouts(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
              
        #Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.completed_workouts a
        USING completed_workouts_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "completed_workouts_delta")
    
        df_start = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.workouts")
                         .filter("action = 'start'")                         
                         .selectExpr("user_id", "workout_id", "session_id", "time as start_time")
                         .withWatermark("start_time", "30 seconds")
                         #.dropDuplicates(["user_id", "workout_id", "session_id", "start_time"])
                   )
        
        df_stop = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.workouts")
                         .filter("action = 'stop'")                         
                         .selectExpr("user_id", "workout_id", "session_id", "time as end_time")
                         .withWatermark("end_time", "30 seconds")
                         #.dropDuplicates(["user_id", "workout_id", "session_id", "end_time"])
                   )
        
        # State cleanup - Define a condition to clean the state
        #               - stop must occur within 3 hours of start 
        #               - stop < start + 3 hours
        join_condition = [df_start.user_id == df_stop.user_id, df_start.workout_id==df_stop.workout_id, df_start.session_id==df_stop.session_id, 
                          df_stop.end_time < df_start.start_time + F.expr('interval 3 hour')]         
        
        df_delta = (df_start.join(df_stop, join_condition)
                            .select(df_start.user_id, df_start.workout_id, df_start.session_id, df_start.start_time, df_stop.end_time)
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("append")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/completed_workouts")
                                 .queryName("completed_workouts_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p1")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
    
    
    
    def upsert_workout_bpm(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
              
        #Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.workout_bpm a
        USING workout_bpm_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "workout_bpm_delta")        
        
        df_users = spark.read.table("users")
        
        df_completed_workouts = (spark.readStream
                                      .option("startingVersion", startingVersion)
                                      .option("ignoreDeletes", True)
                                      #.option("withEventTimeOrder", "true")
                                      #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                                      .table(f"{self.catalog}.{self.db_name}.completed_workouts")
                                      .join(df_users, "user_id")
                                      .selectExpr("user_id", "device_id", "workout_id", "session_id", "start_time", "end_time")
                                      .withWatermark("end_time", "30 seconds")
                                 )
        
        df_bpm = (spark.readStream
                       .option("startingVersion", startingVersion)
                       .option("ignoreDeletes", True)
                       #.option("withEventTimeOrder", "true")
                       #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                       .table(f"{self.catalog}.{self.db_name}.heart_rate")
                       .filter("valid = True")                         
                       .selectExpr("device_id", "time", "heartrate")
                       .withWatermark("time", "30 seconds")
                   )
        
        # State cleanup - Define a condition to clean the state
        #               - Workout could be a maximum of three hours
        #               - workout must end within 3 hours of bpm 
        #               - workout.end < bpm.time + 3 hours
        join_condition = [df_completed_workouts.device_id == df_bpm.device_id, 
                          df_bpm.time > df_completed_workouts.start_time, df_bpm.time <= df_completed_workouts.end_time,
                          df_completed_workouts.end_time < df_bpm.time + F.expr('interval 3 hour')] 
        
        df_delta = (df_bpm.join(df_completed_workouts, join_condition)
                          .select("user_id", "workout_id","session_id", "start_time", "end_time", "time", "heartrate")
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("append")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm")
                                 .queryName("workout_bpm_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
       
    def _await_queries(self, once):
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
                
    def upsert(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nExecuting silver layer upsert ...")
        self.upsert_users(once, processing_time)
        self.upsert_gym_logs(once, processing_time)
        self.upsert_user_profile(once, processing_time)
        self.upsert_workouts(once, processing_time)
        self.upsert_heart_rate(once, processing_time)        
        self._await_queries(once)
        print(f"Completed silver layer 1 upsert {int(time.time()) - start} seconds")
        self.upsert_user_bins(once, processing_time)
        self.upsert_completed_workouts(once, processing_time)        
        self._await_queries(once)
        print(f"Completed silver layer 2 upsert {int(time.time()) - start} seconds")
        self.upsert_workout_bpm(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer 3 upsert {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating silver layer records...")
        self.assert_count("users", 5 if sets == 1 else 10)
        self.assert_count("gym_logs", 8 if sets == 1 else 16)
        self.assert_count("user_profile", 5 if sets == 1 else 10)
        self.assert_count("workouts", 16 if sets == 1 else 32)
        self.assert_count("heart_rate", sets * 253801)
        self.assert_count("user_bins", 5 if sets == 1 else 10)
        self.assert_count("completed_workouts", 8 if sets == 1 else 16)
        self.assert_count("workout_bpm", 3968 if sets == 1 else 8192)
        print(f"Silver layer validation completed in {int(time.time()) - start} seconds")     


# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class Gold():
    def __init__(self, env):
        self.Conf = Config() 
        self.test_data_dir = self.Conf.base_dir_data + "/test_data"
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def upsert_workout_bpm_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        #Idempotent - Once a workout session is complete, It doesn't change. So insert only the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.workout_bpm_summary a
        USING workout_bpm_summary_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "workout_bpm_summary_delta")
        
        df_users = spark.read.table(f"{self.catalog}.{self.db_name}.user_bins")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         #.option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.workout_bpm")
                         .withWatermark("end_time", "30 seconds")
                         .groupBy("user_id", "workout_id", "session_id", "end_time")
                         .agg(F.min("heartrate").alias("min_bpm"), F.mean("heartrate").alias("avg_bpm"), 
                              F.max("heartrate").alias("max_bpm"), F.count("heartrate").alias("num_recordings"))                         
                         .join(df_users, ["user_id"])
                         .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm", "max_bpm", "num_recordings")
                     )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("append")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm_summary")
                                 .queryName("workout_bpm_summary_upsert_stream")
                        )
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "gold_p1")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
    
    
    def upsert(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nExecuting gold layer upsert ...")
        self.upsert_workout_bpm_summary(once, processing_time)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        print(f"Completed gold layer upsert {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success") 
        
    def assert_rows(self, location, table_name, sets):
        print(f"Validating records in {table_name}...", end='')
        expected_rows = spark.read.format("parquet").load(f"{self.test_data_dir}/{location}_{sets}.parquet").collect()
        actual_rows = spark.table(table_name).collect()
        assert expected_rows == actual_rows, f"Expected data mismatches with the actual data in {table_name}"
        print(f"Expected data matches with the actual data in {table_name}: Success")
        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating gold layer records..." )       
        self.assert_rows("7-gym_summary", "gym_summary", sets)       
        if sets>1:
            self.assert_count("workout_bpm_summary", 2)
        print(f"Gold layer validation completed in {int(time.time()) - start} seconds")        


# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
once = True if dbutils.widgets.get("RunType")=="once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print(f"Starting sbit in batch mode.")
else:
    print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

SH = SetupHelper(env)
HL = HistoryLoader(env)

# COMMAND ----------

setup_required = spark.sql(f"SHOW DATABASES IN {SH.catalog}").filter(f"databaseName == '{SH.db_name}'").count() != 1
if setup_required:
    SH.setup()
    SH.validate()
    HL.load_history()
    HL.validate()
else:
    spark.sql(f"USE {SH.catalog}.{SH.db_name}")

# COMMAND ----------

# MAGIC %run ./04-bronze

# COMMAND ----------

# MAGIC %run ./05-silver

# COMMAND ----------

# MAGIC %run ./06-gold

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

# COMMAND ----------

BZ.consume(once, processing_time)

# COMMAND ----------

SL.upsert(once, processing_time)

# COMMAND ----------

GL.upsert(once, processing_time)


# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

HL = HistoryLoader(env)
SH.validate()
HL.validate()

# COMMAND ----------

# MAGIC %run ./10-producer

# COMMAND ----------

PR =Producer()
PR.produce(1)
PR.validate(1)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %run ./04-bronze

# COMMAND ----------

# MAGIC %run ./05-silver

# COMMAND ----------

# MAGIC %run ./06-gold

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)
BZ.validate(1)
SL.validate(1)
GL.validate(1)

# COMMAND ----------

PR.produce(2)
PR.validate(2)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

BZ.validate(2)
SL.validate(2)
GL.validate(2)
SH.cleanup()


# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("Host", "", "Databricks Workspace URL")
dbutils.widgets.text("AccessToken", "", "Secure Access Token")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
host = dbutils.widgets.get("Host")
token = dbutils.widgets.get("AccessToken")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

job_payload = \
{
        "name": "stream-test",
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "stream-test-task",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "/Repos/SBIT/SBIT/07-run",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1
                    },
                    "node_type_id": "Standard_DS4_v2",
                    "driver_node_type_id": "Standard_DS4_v2",
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
    }

# COMMAND ----------

# Create a streaming job
import requests
import json
create_response = requests.post(host + '/api/2.1/jobs/create', data=json.dumps(job_payload), auth=("token", token))
print(f"Response: {create_response}")
job_id = json.loads(create_response.content.decode('utf-8'))["job_id"]
print(f"Created Job {job_id}")

# COMMAND ----------

# Trigger the streaming job
run_payload = {"job_id": job_id, "notebook_params": {"Environment":env, "RunType": "stream", "ProcessingTime": "1 seconds"}}
run_response = requests.post(host + '/api/2.1/jobs/run-now', data=json.dumps(run_payload), auth=("token", token))
run_id = json.loads(run_response.content.decode('utf-8'))["run_id"]
print(f"Started Job run {run_id}")

# COMMAND ----------

# Wait until job starts
import time
status_payload = {"run_id": run_id}
job_status="PENDING"
while job_status == "PENDING":
    time.sleep(20)
    status_job_response = requests.get(host + '/api/2.1/jobs/runs/get', data=json.dumps(status_payload), auth=("token", token))
    job_status = json.loads(status_job_response.content.decode('utf-8'))["tasks"][0]["state"]["life_cycle_state"]  
    print(job_status)    

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

# MAGIC %run ./10-producer

# COMMAND ----------

# MAGIC %run ./04-bronze

# COMMAND ----------

# MAGIC %run ./05-silver

# COMMAND ----------

# MAGIC %run ./06-gold

# COMMAND ----------

import time

print("Sleep for 2 minutes and let setup and history loader finish...")
time.sleep(2*60)

#Validate setup and history load
HL = HistoryLoader(env)
PR = Producer()
BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

SH.validate()
HL.validate()

#Produce some incremantal
PR.produce(1)
PR.validate(1)

# COMMAND ----------

print("Sleep for 2 minutes and let microbatch pickup the data...")
time.sleep(2*60)

#Validate bronze, silver and gold layer 
BZ.validate(1)
SL.validate(1)
GL.validate(1)
 

#Produce some incremantal data and wait for micro batch
PR.produce(2)
PR.validate(2)

# COMMAND ----------

print("Sleep for 2 minutes and let microbatch pickup the data...")
time.sleep(2*60)

#Validate bronze, silver and gold layer 
BZ.validate(2)
SL.validate(2)
GL.validate(2)

# COMMAND ----------

#Terminate the streaming Job
cancel_payload = {"run_id": run_id}
cancel_response = requests.post(host + '/api/2.1/jobs/runs/cancel', data=json.dumps(cancel_payload), auth=("token", token))
print(f"Canceled Job run {run_id}. Status {cancel_response}")

# COMMAND ----------

#Delete the Job
delete_job_payload = {"job_id": job_id}
delete_job_response = requests.post(host + '/api/2.1/jobs/delete', data=json.dumps(delete_job_payload), auth=("token", token))
print(f"Canceled Job run {run_id}. Status {delete_job_response}")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")


# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Producer():
    def __init__(self):
        self.Conf = Config()
        self.landing_zone = self.Conf.base_dir_data + "/raw"      
        self.test_data_dir = self.Conf.base_dir_data + "/test_data"
               
    def user_registration(self, set_num):
        source = f"{self.test_data_dir}/1-registered_users_{set_num}.csv"
        target = f"{self.landing_zone}/registered_users_bz/1-registered_users_{set_num}.csv" 
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def profile_cdc(self, set_num):
        source = f"{self.test_data_dir}/2-user_info_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/2-user_info_{set_num}.json"
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")        
        
    def workout(self, set_num):
        source = f"{self.test_data_dir}/4-workout_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/4-workout_{set_num}.json"
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def bpm(self, set_num):
        source = f"{self.test_data_dir}/3-bpm_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/3-bpm_{set_num}.json"
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def gym_logins(self, set_num):
        source = f"{self.test_data_dir}/5-gym_logins_{set_num}.csv"
        target = f"{self.landing_zone}/gym_logins_bz/5-gym_logins_{set_num}.csv" 
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def produce(self, set_num):
        import time
        start = int(time.time())
        print(f"\nProducing test data set {set_num} ...")
        if set_num <=2:
            self.user_registration(set_num)
            self.profile_cdc(set_num)        
            self.workout(set_num)
            self.gym_logins(set_num)
        if set_num <=10:
            self.bpm(set_num)
        print(f"Test data set {set_num} produced in {int(time.time()) - start} seconds")
    
    def _validate_count(self, format, location, expected_count):
        print(f"Validating {location}...", end='')
        target = f"{self.landing_zone}/{location}_*.{format}"
        actual_count = (spark.read
                             .format(format)
                             .option("header","true")
                             .load(target).count())
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {location}"
        print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")
          
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating test data {sets} sets...")       
        self._validate_count("csv", "registered_users_bz/1-registered_users", 5 if sets == 1 else 10)
        self._validate_count("json","kafka_multiplex_bz/2-user_info", 7 if sets == 1 else 13)
        self._validate_count("json","kafka_multiplex_bz/3-bpm", sets * 253801)
        self._validate_count("json","kafka_multiplex_bz/4-workout", 16 if sets == 1 else 32)  
        self._validate_count("csv", "gym_logins_bz/5-gym_logins", 8 if sets == 1 else 16)
        #print(f"Test data validation completed in {int(time.time()) - start} seconds")

import sys
import requests
import json


def get_job_definition():
    job_def = \
        {"name": "ingest-process-pipeline",
         "email_notifications": {
             "no_alert_for_skipped_runs": "false"
         },
         "webhook_notifications": {},
         "timeout_seconds": 0,
         "max_concurrent_runs": 1,
         "tasks": [
             {
                 "task_key": "ingest-data",
                 "run_if": "ALL_SUCCESS",
                 "notebook_task": {
                     "notebook_path": "/exercise/data_ingestion",
                     "source": "WORKSPACE"
                 },
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "timeout_seconds": 0,
                 "email_notifications": {},
                 "notification_settings": {
                     "no_alert_for_skipped_runs": "false",
                     "no_alert_for_canceled_runs": "false",
                     "alert_on_last_attempt": "false"
                 }
             },
             {
                 "task_key": "process-data-2014",
                 "depends_on": [
                     {
                         "task_key": "ingest-data"
                     }
                 ],
                 "run_if": "ALL_SUCCESS",
                 "notebook_task": {
                     "notebook_path": "/exercise/data-analysis",
                     "base_parameters": {
                         "year": "2014"
                     },
                     "source": "WORKSPACE"
                 },
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "timeout_seconds": 0,
                 "email_notifications": {},
                 "notification_settings": {
                     "no_alert_for_skipped_runs": "false",
                     "no_alert_for_canceled_runs": "false",
                     "alert_on_last_attempt": "false"
                 }
             },
             {
                 "task_key": "process-data-2015",
                 "depends_on": [
                     {
                         "task_key": "ingest-data"
                     }
                 ],
                 "run_if": "ALL_SUCCESS",
                 "notebook_task": {
                     "notebook_path": "/exercise/data-analysis",
                     "base_parameters": {
                         "year": "2015"
                     },
                     "source": "WORKSPACE"
                 },
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "timeout_seconds": 0,
                 "email_notifications": {}
             }
         ],
         "job_clusters": [
             {
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "new_cluster": {
                     "cluster_name": "",
                     "spark_version": "14.0.x-scala2.12",
                     "spark_conf": {
                         "spark.databricks.delta.preview.enabled": "true",
                         "spark.master": "local[*, 4]",
                         "spark.databricks.cluster.profile": "singleNode"
                     },
                     "azure_attributes": {
                         "first_on_demand": 1,
                         "availability": "ON_DEMAND_AZURE",
                         "spot_bid_max_price": -1
                     },
                     "node_type_id": "Standard_DS3_v2",
                     "custom_tags": {
                         "ResourceClass": "SingleNode"
                     },
                     "enable_elastic_disk": "true",
                     "data_security_mode": "SINGLE_USER",
                     "runtime_engine": "STANDARD",
                     "num_workers": 0
                 }
             }
         ],
         "format": "MULTI_TASK"
         }

    return json.dumps(job_def)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Deploy <URL> <Token>")
        sys.exit(-1)

    host = sys.argv[1]
    token = sys.argv[2]

    create_job_response = requests.post(host +
                                        '/api/2.1/jobs/create',
                                        data=get_job_definition(),
                                        auth=("token", token))

    job_id = json.loads(create_job_response.content.decode('utf-8'))["job_id"]
    print(f"Successfully Deployed Job ID: {job_id}")
