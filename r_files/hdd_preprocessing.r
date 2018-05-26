#################################################################################
# SparkR script to reduce the original 3 year hard disk drive Blackblaze dataset
#################################################################################

#############################################
# Initialize Spark Context and configuration
#############################################
library(SparkR)
# spark and sparksql context
sc = sparkR.session("local[*]")
sqlContext <- sparkRSQL.init(sc)
sql(sqlContext,"set spark.sql.shuffle.partitions=1")

########################################################################################
# Filter master dataset into equivalent failures and operational hard drives -- sample
#######################################################################################
# load dataframe
raw_dataset <- loadDF(sqlContext, path="D:\\scripts\\dataset\\Blackblaze HDD Failure\\data_Q4_2016", source="csv", header="true", inferSchema=TRUE)

# register as temp table
registerTempTable(raw_dataset, "raw_dataset")

# filter by failure drives
#failure_distribution_days<- collect(sql(sqlContext, "SELECT serial_number,model,(int(smart_9_raw)/24) AS lifetime FROM raw_dataset WHERE failure=1 ORDER BY lifetime"))

# selected list of drives
filtered_failure_disks <- loadDF(sqlContext, path="D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\failure_distribution_days_sample.csv", source="csv", header="true", inferSchema=TRUE)
registerTempTable(filtered_failure_disks, "filtered_failure_disks")

# filter only drives that were working for more than 6 months
filtered_failure_disks_six_months <- sql(sqlContext, "SELECT * from filtered_failure_disks WHERE lifetime >= 180")

# collect full data of drives by left outer join
filtered_failure_disks_full_data <- sql(sqlContext, "SELECT raw_dataset.serial_number, raw_dataset.date, raw_dataset.model, raw_dataset.capacity_bytes, raw_dataset.failure, smart_1_normalized, smart_1_raw, smart_2_normalized, smart_2_raw, smart_3_normalized, smart_3_raw, smart_4_normalized, smart_4_raw, smart_5_normalized, smart_5_raw, smart_7_normalized, smart_7_raw, smart_8_normalized, smart_8_raw, smart_9_normalized, smart_9_raw, smart_10_normalized, smart_10_raw, smart_11_normalized, smart_11_raw, smart_12_normalized, smart_12_raw, smart_13_normalized, smart_13_raw, smart_15_normalized, smart_15_raw, smart_22_normalized, smart_22_raw, smart_183_normalized, smart_183_raw, smart_184_normalized, smart_184_raw, smart_187_normalized, smart_187_raw, smart_188_normalized, smart_188_raw, smart_189_normalized, smart_189_raw, smart_190_normalized, smart_190_raw, smart_191_normalized, smart_191_raw, smart_192_normalized, smart_192_raw, smart_193_normalized, smart_193_raw, smart_194_normalized, smart_194_raw, smart_195_normalized, smart_195_raw, smart_196_normalized, smart_196_raw, smart_197_normalized, smart_197_raw, smart_198_normalized, smart_198_raw, smart_199_normalized, smart_199_raw, smart_200_normalized, smart_200_raw, smart_201_normalized, smart_201_raw, smart_220_normalized, smart_220_raw, smart_222_normalized, smart_222_raw, smart_223_normalized, smart_223_raw, smart_224_normalized, smart_224_raw, smart_225_normalized, smart_225_raw, smart_226_normalized, smart_226_raw, smart_240_normalized, smart_240_raw, smart_241_normalized, smart_241_raw, smart_242_normalized, smart_242_raw, smart_250_normalized, smart_250_raw, smart_251_normalized, smart_251_raw, smart_252_normalized, smart_252_raw, smart_254_normalized, smart_254_raw, smart_255_normalized, smart_255_raw from raw_dataset RIGHT OUTER JOIN filtered_failure_disks ON raw_dataset.serial_number = filtered_failure_disks.serial_number")
write.df(filtered_failure_disks_full_data,"D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\hdd_filtered_dataset_2016q4", source="csv")


###########################################################################
# Sort filtered dataset (Seagate model) by date and group by serial_number
###########################################################################
hdd_2015_2017 <- loadDF(sqlContext, path="D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\seagate_model_ST4000DM000.csv", source="csv", header="true", inferSchema=TRUE)
registerTempTable(hdd_2015_2017, "hdd_2015_2017")

#sorted_serial_number_hdd_2017 <- arrange(hdd_2017, desc(hdd_2017$date))
#schema <- schema(hdd_2017)
#result <- gapply(
#  hdd_2017,
#  "serial_number",
#  function(key, x) {
#    y <- data.frame(key, x)
#  },
#  schema)
#sorted_serial_number_hdd_2017 <- arrange(result, "date", decreasing = TRUE)


sorted_serial_number_hdd_2015_2017 <- sql(sqlContext, "SELECT serial_number, date, model, capacity_bytes, failure, smart_1_normalized, smart_1_raw, smart_2_normalized, smart_2_raw, smart_3_normalized, smart_3_raw, smart_4_normalized, smart_4_raw, smart_5_normalized, smart_5_raw, smart_7_normalized, smart_7_raw, smart_8_normalized, smart_8_raw, smart_9_normalized, smart_9_raw, smart_10_normalized, smart_10_raw, smart_11_normalized, smart_11_raw, smart_12_normalized, smart_12_raw, smart_13_normalized, smart_13_raw, smart_15_normalized, smart_15_raw, smart_22_normalized, smart_22_raw, smart_183_normalized, smart_183_raw, smart_184_normalized, smart_184_raw, smart_187_normalized, smart_187_raw, smart_188_normalized, smart_188_raw, smart_189_normalized, smart_189_raw, smart_190_normalized, smart_190_raw, smart_191_normalized, smart_191_raw, smart_192_normalized, smart_192_raw, smart_193_normalized, smart_193_raw, smart_194_normalized, smart_194_raw, smart_195_normalized, smart_195_raw, smart_196_normalized, smart_196_raw, smart_197_normalized, smart_197_raw, smart_198_normalized, smart_198_raw, smart_199_normalized, smart_199_raw, smart_200_normalized, smart_200_raw, smart_201_normalized, smart_201_raw, smart_220_normalized, smart_220_raw, smart_222_normalized, smart_222_raw, smart_223_normalized, smart_223_raw, smart_224_normalized, smart_224_raw, smart_225_normalized, smart_225_raw, smart_226_normalized, smart_226_raw, smart_240_normalized, smart_240_raw, smart_241_normalized, smart_241_raw, smart_242_normalized, smart_242_raw, smart_250_normalized, smart_250_raw, smart_251_normalized, smart_251_raw, smart_252_normalized, smart_252_raw, smart_254_normalized, smart_254_raw, smart_255_normalized, smart_255_raw FROM (SELECT serial_number, date, model, capacity_bytes, failure, smart_1_normalized, smart_1_raw, smart_2_normalized, smart_2_raw, smart_3_normalized, smart_3_raw, smart_4_normalized, smart_4_raw, smart_5_normalized, smart_5_raw, smart_7_normalized, smart_7_raw, smart_8_normalized, smart_8_raw, smart_9_normalized, smart_9_raw, smart_10_normalized, smart_10_raw, smart_11_normalized, smart_11_raw, smart_12_normalized, smart_12_raw, smart_13_normalized, smart_13_raw, smart_15_normalized, smart_15_raw, smart_22_normalized, smart_22_raw, smart_183_normalized, smart_183_raw, smart_184_normalized, smart_184_raw, smart_187_normalized, smart_187_raw, smart_188_normalized, smart_188_raw, smart_189_normalized, smart_189_raw, smart_190_normalized, smart_190_raw, smart_191_normalized, smart_191_raw, smart_192_normalized, smart_192_raw, smart_193_normalized, smart_193_raw, smart_194_normalized, smart_194_raw, smart_195_normalized, smart_195_raw, smart_196_normalized, smart_196_raw, smart_197_normalized, smart_197_raw, smart_198_normalized, smart_198_raw, smart_199_normalized, smart_199_raw, smart_200_normalized, smart_200_raw, smart_201_normalized, smart_201_raw, smart_220_normalized, smart_220_raw, smart_222_normalized, smart_222_raw, smart_223_normalized, smart_223_raw, smart_224_normalized, smart_224_raw, smart_225_normalized, smart_225_raw, smart_226_normalized, smart_226_raw, smart_240_normalized, smart_240_raw, smart_241_normalized, smart_241_raw, smart_242_normalized, smart_242_raw, smart_250_normalized, smart_250_raw, smart_251_normalized, smart_251_raw, smart_252_normalized, smart_252_raw, smart_254_normalized, smart_254_raw, smart_255_normalized, smart_255_raw,dense_rank() OVER (PARTITION BY serial_number ORDER BY date DESC) as rank FROM hdd_2015_2017) tmp WHERE rank <=30")
printSchema(sorted_serial_number_hdd_2015_2017)
write.df(sorted_serial_number_hdd_2015_2017,"D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\seagate_model_ST4000DM000_2015_2017_sorted_30days", source="csv", header=TRUE)

#############################################################################################
# For subset of 30 day dataset into various smaller days (change rank value for last N days)
#############################################################################################
hdd_subset <- loadDF(sqlContext, path="D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\seagate_model_ST4000DM000_2015_2017_sorted_30days\\seagate_model_ST4000DM000_2015_2017_sorted_30day.csv", source="csv", header="true", inferSchema=TRUE)
registerTempTable(hdd_subset, "hdd_subset")
sorted_serial_number_hdd_subset <- sql(sqlContext, "SELECT serial_number, date, failure, smart_1_normalized, smart_1_raw, smart_3_normalized, smart_3_raw, smart_4_normalized, smart_4_raw, smart_5_normalized, smart_5_raw, smart_7_normalized, smart_7_raw, smart_9_normalized, smart_9_raw, smart_10_normalized, smart_10_raw, smart_12_normalized, smart_12_raw, smart_183_normalized, smart_183_raw, smart_184_normalized, smart_184_raw, smart_187_normalized, smart_187_raw, smart_188_normalized, smart_188_raw, smart_189_normalized, smart_189_raw, smart_190_normalized, smart_190_raw, smart_192_normalized, smart_192_raw, smart_193_normalized, smart_193_raw, smart_194_normalized, smart_194_raw, smart_197_normalized, smart_197_raw, smart_198_normalized, smart_198_raw, smart_199_normalized, smart_199_raw,smart_240_normalized, smart_240_raw, smart_241_normalized, smart_241_raw, smart_242_normalized, smart_242_raw FROM (SELECT serial_number, date, failure, smart_1_normalized, smart_1_raw, smart_3_normalized, smart_3_raw, smart_4_normalized, smart_4_raw, smart_5_normalized, smart_5_raw, smart_7_normalized, smart_7_raw, smart_9_normalized, smart_9_raw, smart_10_normalized, smart_10_raw, smart_12_normalized, smart_12_raw, smart_183_normalized, smart_183_raw, smart_184_normalized, smart_184_raw, smart_187_normalized, smart_187_raw, smart_188_normalized, smart_188_raw, smart_189_normalized, smart_189_raw, smart_190_normalized, smart_190_raw, smart_192_normalized, smart_192_raw, smart_193_normalized, smart_193_raw, smart_194_normalized, smart_194_raw, smart_197_normalized, smart_197_raw, smart_198_normalized, smart_198_raw, smart_199_normalized, smart_199_raw, smart_240_normalized, smart_240_raw, smart_241_normalized, smart_241_raw, smart_242_normalized, smart_242_raw, dense_rank() OVER (PARTITION BY serial_number ORDER BY date DESC) as rank FROM hdd_subset) tmp WHERE rank <=15")
write.df(sorted_serial_number_hdd_subset,"D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\seagate_model_ST4000DM000_2015_2017_sorted_15days", source="csv", header=TRUE)