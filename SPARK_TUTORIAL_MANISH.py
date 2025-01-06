# Databricks notebook source
# MAGIC %md
# MAGIC read_csv_file

# COMMAND ----------

# /FileStore/tables/2010_summary.csv

# COMMAND ----------

spark

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header","false")\
        .option("inferSchema","false")\
            .option("mode","FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")

flight_df.show()

# COMMAND ----------

flight_df_header=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","false")\
            .option("mode","FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")

flight_df_header.show()

# COMMAND ----------

flight_df_header.printSchema()

# COMMAND ----------

flight_df_header_schema=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
            .option("mode","FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")

flight_df_header_schema.show(5)

# COMMAND ----------

flight_df_header_schema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Schema in Spark
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

my_schema= StructType([
            StructField("DEST_COUNTRY_NAME", StringType(), True),
            StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
            StructField("count", IntegerType(), True)
])

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header","false")\
        .option("inferSchema","false")\
          .schema(my_schema)\
            .option("mode","FAILFAST")\
                .load("/FileStore/tables/2010_summary.csv")

flight_df.show(5)

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header","false")\
        .option("inferSchema","false")\
          .schema(my_schema)\
            .option("mode","PERMISSIVE")\
                .load("/FileStore/tables/2010_summary.csv")

flight_df.show(5)

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header","false")\
        .option("skipRows",1)\
        .option("inferSchema","false")\
          .schema(my_schema)\
            .option("mode","PERMISSIVE")\
                .load("/FileStore/tables/2010_summary.csv")

flight_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Handling Corrupted Record

# COMMAND ----------

 /FileStore/tables/employee.csv

# COMMAND ----------

employee_df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
            .option("mode","PERMISSIVE")\
                .load("/FileStore/tables/employee.csv")

employee_df.show(5)

# COMMAND ----------

employee_df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
            .option("mode","DROPMALFORMED")\
                .load("/FileStore/tables/employee.csv")

employee_df.show(5)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

emp_schema= StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("nominee", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

employee_df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
            .option("mode","PERMISSIVE")\
                .schema(emp_schema)\
                .load("/FileStore/tables/employee.csv")

employee_df.show(truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

employee_df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
                .schema(emp_schema)\
                    .option("badRecordsPath","/FileStore/tables/bad_records")\
                .load("/FileStore/tables/employee.csv")

employee_df.show(truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bad_records/20241219T155908/bad_records/

# COMMAND ----------

bad_data_df= spark.read.format('json')\
    .load('/FileStore/tables/bad_records/20241219T155908/bad_records/')

bad_data_df.show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Read JSON File

# COMMAND ----------

File uploaded to /FileStore/tables/corrupted_json.json
File uploaded to /FileStore/tables/line_delimited_json.json
File uploaded to /FileStore/tables/Multi_line_correct.json
File uploaded to /FileStore/tables/Multi_line_incorrect.json
File uploaded to /FileStore/tables/single_file_json_with_extra_fields.json

# COMMAND ----------

spark.read.format('json')\
    .option('inferschema','true')\
        .option('mode','PERMISSIVE')\
            .load('/FileStore/tables/line_delimited_json.json').show()

# COMMAND ----------

spark

# COMMAND ----------

spark.read.format('json')\
    .option('inferschema','true')\
        .option('mode','PERMISSIVE')\
            .load('/FileStore/tables/single_file_json_with_extra_fields.json').show()

# COMMAND ----------

spark.read.format('json')\
    .option('inferschema','true')\
        .option('mode','PERMISSIVE')\
            .option('multiline','true')\
            .load('/FileStore/tables/Multi_line_correct.json').show()

# COMMAND ----------

spark.read.format('json')\
    .option('inferschema','true')\
        .option('mode','PERMISSIVE')\
            .option('multiline','true')\
            .load('/FileStore/tables/Multi_line_incorrect.json').show()

# COMMAND ----------

spark.read.format('json')\
    .option('inferschema','true')\
        .option('mode','PERMISSIVE')\
            .load('/FileStore/tables/corrupted_json.json').show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Parquet file

# COMMAND ----------

File uploaded to /FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet


# COMMAND ----------

df = spark.read.parquet('/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write data  /FileStore/tables/person.csv

# COMMAND ----------

df=spark.read.format('csv')\
  .option('header','true')\
    .option('inferschema','true')\
      .load('/FileStore/tables/employees.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format('csv')\
  .option('header','true')\
    .option('mode','overwrite')\
      .option('path','/FileStore/tables/csv_write/')\
          .save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/csv_write/')

# COMMAND ----------

df.repartition(3).write.format('csv')\
  .option('header','true')\
    .option('mode','overwrite')\
      .option('path','/FileStore/tables/csv_write_repartition/')\
          .save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/csv_write_repartition/')

# COMMAND ----------

# MAGIC %md
# MAGIC Partitioning and Bucketing

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format('csv')\
  .option('header','true')\
    .option('mode','overwrite')\
      .option('path','/FileStore/tables/partitionBy_address/')\
       .partitionBy('address')\
          .save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/partitionBy_address/')

# COMMAND ----------

df.write.format('csv')\
  .option('header','true')\
    .option('mode','overwrite')\
      .option('path','/FileStore/tables/partitionBy_address_gender/')\
       .partitionBy('address','gender')\
          .save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/partitionBy_address_gender/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables/partitionBy_address_gender/address=INDIA/')

# COMMAND ----------

df.write.format('csv')\
  .option('header','true')\
    .option('mode','overwrite')\
      .option('path','/FileStore/tables/bucket_by_id/')\
       .bucketBy(3,'id')\
          .saveAsTable('bucket_by_id_table')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/bucket_by_id/')

# COMMAND ----------

# MAGIC %md
# MAGIC Dataframe

# COMMAND ----------

my_data = [(1,1),(2,1),(3,1),(4,2),(5,1),(6,2),(7,2)]

# COMMAND ----------

my_schema = ['id','num']

# COMMAND ----------

my_df=spark.createDataFrame(data=my_data,schema=my_schema)

# COMMAND ----------

my_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------

employee_df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
            .option("mode","PERMISSIVE")\
                .load("/FileStore/tables/employee.csv")

employee_df.show()

# COMMAND ----------

employee_df.select('name').show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

employee_df.select(col('name')).show()

# COMMAND ----------

employee_df.select('id + 5').show()

# COMMAND ----------

employee_df.select(col('id') + 5).show()

# COMMAND ----------

employee_df.select('id','name','age').show()

# COMMAND ----------

employee_df.select(col('id'),col('name'),col('age')).show()

# COMMAND ----------

employee_df.select('id',col('name'),employee_df['salary'],employee_df.address).show()

# COMMAND ----------

employee_df.select(expr('id + 5')).show()

# COMMAND ----------

employee_df.select(expr('id as emp_id'),expr('name as emp_name'),expr('concat(name,address)')).show()

# COMMAND ----------

employee_df.select(expr('id as emp_id'),expr('name as emp_name'),expr('concat(name,address,"@gmail.com") as email')).show(truncate=False)

# COMMAND ----------

employee_df.createOrReplaceTempView('emp_tbl')

# COMMAND ----------

spark.sql(""" select * from emp_tbl """).show()

# COMMAND ----------

spark.sql(""" select id,name,age from emp_tbl """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation - 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

employee_df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
            .option("mode","PERMISSIVE")\
                .load("/FileStore/tables/employee.csv")

employee_df.show()

employee_df.printSchema()

employee_df.createOrReplaceTempView('emp_tbl')

# COMMAND ----------

# Alias

employee_df.select(col('id').alias('emp_id'),'name','age').show()

# COMMAND ----------

# Filter

employee_df.filter(col('salary')>150000).show()

# COMMAND ----------

# Filter

employee_df.filter(col('salary')>150000 & col('age')<18).show()

# COMMAND ----------

# Filter

employee_df.filter((col('salary')>150000) & (col('age')<18)).show()

# COMMAND ----------

# Where
employee_df.where(col('salary')>150000).show()

# COMMAND ----------

# Literal

employee_df.select('*',lit('Kumar').alias('Last_name')).show()

# COMMAND ----------

# Add Column
employee_df.withColumn('surname',lit('Singh')).show()

# COMMAND ----------

# Rename column

employee_df.withColumnRenamed('id','emp_id').show()

# COMMAND ----------

# Casting

employee_df.printSchema()

employee_df.withColumn('id',col('id').cast('string')).printSchema()

# COMMAND ----------

employee_df.withColumn('id',col('id').cast('string'))\
    .withColumn('salary',col('salary').cast('long'))\
    .printSchema()

# COMMAND ----------

employee_df.drop('id',col('name')).show()

# COMMAND ----------

# Spark SQL

# COMMAND ----------

spark.sql(""" select * from emp_tbl where salary>150000 and age<18 """).show()

spark.sql(""" select *,'Kumar' as last_name from emp_tbl where salary>150000 and age<18 """).show()

spark.sql(""" select *,'Kumar' as last_name , concat(name,' ',last_name) as full_name from emp_tbl where salary>150000 and age<18 """).show()

spark.sql(""" select *,'Kumar' as last_name , concat(name,' ',last_name) as full_name, id as emp_id from emp_tbl where salary>150000 and age<18 """).show()

spark.sql(""" select *,'Kumar' as last_name , concat(name,' ',last_name) as full_name, cast(id as string) from emp_tbl where salary>150000 and age<18 """).printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Union

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]

schema = ['id','Name','sal','mngr_id']

manager_df=spark.createDataFrame(data,schema)

# COMMAND ----------

manager_df.show()

# COMMAND ----------

manager_df.count()

# COMMAND ----------

data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17),
(20 ,'Sima',75000,  17),(20 ,'Sima',75000,  17)]

schema1 = ['id','Name','sal','mngr_id']

manager_df1=spark.createDataFrame(data1,schema1)

# COMMAND ----------

manager_df1.show()

# COMMAND ----------

manager_df.union(manager_df1).show()

# COMMAND ----------

manager_df.union(manager_df1).count()

# COMMAND ----------

manager_df.unionAll(manager_df1).count() # No diff

# COMMAND ----------

manager_df.createOrReplaceTempView('mngr_tbl')

manager_df1.createOrReplaceTempView('mngr_tbl1')

# COMMAND ----------

spark.sql(""" select * from mngr_tbl
          union
          select * from mngr_tbl1 """).count()

# COMMAND ----------

spark.sql(""" select * from mngr_tbl
          union all
          select * from mngr_tbl1 """).count()

# COMMAND ----------

spark.sql(""" select * from mngr_tbl1
          union 
          select * from mngr_tbl1 """).count()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]

wrong_schema = ['id','sal','mngr_id','Name']

wrong_manager_df=spark.createDataFrame(wrong_column_data,wrong_schema)

# COMMAND ----------

manager_df1.union(wrong_manager_df).show()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]

wrong_schema = ['id','sal','mngr_id','Name','bonus']

wrong_manager_df=spark.createDataFrame(wrong_column_data,wrong_schema)

# COMMAND ----------

manager_df1.union(wrong_manager_df).show()

# COMMAND ----------

wrong_manager_df.select('id','sal','mngr_id','Name').union(manager_df1).show()

# COMMAND ----------

manager_df1.unionByName(wrong_manager_df).show()

# COMMAND ----------

wrong_column_data1=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]

wrong_schema1 = ['id','sal','mngr_id','Nam']

wrong_manager_df1=spark.createDataFrame(wrong_column_data1,wrong_schema1)

# COMMAND ----------

wrong_manager_df.unionByName(wrong_manager_df1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC If else

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

schema=['id','name','age','salary','country','dept']

emp_df=spark.createDataFrame(emp_data,schema)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_df.withColumn("adult",when(col('age')<18,'No')
                  .when(col('age')>18,'Yes')
                  .otherwise('NoValue')).show()

# COMMAND ----------

emp_df.withColumn('age',when(col('age').isNull(),lit(19)).otherwise(col('age')))\
.withColumn("adult",when(col('age')<18,'No').otherwise('Yes')
                  ).show()

# COMMAND ----------

emp_df.withColumn('age_wise',when(col('age')>0 & col('age')<18,'Minor')).show()

# COMMAND ----------

emp_df.withColumn('age_wise',when((col('age')>0) & (col('age')<18),'Minor')
                  .when((col('age')>18) & (col('age')<30),'Mid')
                  .otherwise('Major'))\
                  .show()

# COMMAND ----------

emp_df.createOrReplaceTempView('emp_tbl')

# COMMAND ----------

spark.sql("""
          select *,
          case when age<18 then 'Minor'
          when age>18 then 'Major'
          else 'NoValue'
          end as adult
          from emp_tbl
          """).show()

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]
schema = ['id','Name','sal','mngr_id']

manager_df=spark.createDataFrame(data,schema)

# COMMAND ----------

manager_df.show()

# COMMAND ----------

manager_df.count()

# COMMAND ----------

manager_df.distinct().show()

# COMMAND ----------

manager_df.distinct().count()

# COMMAND ----------

manager_df.distinct('id','name').show()

# COMMAND ----------

manager_df.select('id','name').distinct().show()

# COMMAND ----------

manager_df.drop_duplicates().show()

# COMMAND ----------

manager_df.dropDuplicates().show()

# COMMAND ----------

dropped_mngr_df = manager_df.dropDuplicates()

# COMMAND ----------

manager_df.sort('sal').show()

# COMMAND ----------

manager_df.sort(col('sal').desc()).show()

# COMMAND ----------

manager_df.sort(col('sal').desc(),col('Name').desc()).show()

# COMMAND ----------

leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]

leet_code_schema = ['id','name','referee_id']

cust_df = spark.createDataFrame(leet_code_data,leet_code_schema)

# COMMAND ----------

cust_df.show()

# COMMAND ----------

cust_df.select('name').where((col('referee_id') != 2) |(col('referee_id').isNull())) .show() # where referee_id is not 2

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregate Function

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

emp_schema = ['id','name','age','salary','country','dept']

emp_df=spark.createDataFrame(emp_data,emp_schema)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

emp_df.count()

# COMMAND ----------

emp_df.select(count('name')).show()

# COMMAND ----------

emp_df.select(count('id')).show()

# COMMAND ----------

emp_df.select(count('*')).show()

# COMMAND ----------

emp_df.select(sum('salary'),min('salary'),max('salary'),avg('salary')).show()

# COMMAND ----------

emp_df.select(sum('salary').alias('total_salary'),min('salary').alias('minimum_salary'),max('salary').alias('maximum_salary'),avg('salary').alias('average_salary')).show()

# COMMAND ----------

emp_df.select(sum('salary').alias('total_salary'),min('salary').alias('minimum_salary'),max('salary').alias('maximum_salary'),avg('salary').cast('integer').alias('average_salary')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Group By

# COMMAND ----------

emp_data=[(1,'manish',50000,"IT"),
(2,'vikash',60000,'sales'),
(3,'raushan',70000,'marketing'),
(4,'mukesh',80000,'IT'),
(5,'pritam',90000,'sales'),
(6,'nikita',45000,'marketing'),
(7,'ragini',55000,'marketing'),
(8,'rakesh',100000,'IT'),
(9,'aditya',65000,'IT'),
(10,'rahul',50000,'marketing')]

emp_schema = ['id','name','salary','dept']

emp_df=spark.createDataFrame(emp_data,emp_schema)

emp_df.show()

# COMMAND ----------

emp_df.describe().show()

# COMMAND ----------

emp_df.groupBy('dept')\
    .agg(sum('salary')).show()

# COMMAND ----------

emp_data=[(1,'manish',50000,"IT",'india'),
(2,'vikash',60000,'sales','us'),
(3,'raushan',70000,'marketing','india'),
(4,'mukesh',80000,'IT','us'),
(5,'pritam',90000,'sales','india'),
(6,'nikita',45000,'marketing','us'),
(7,'ragini',55000,'marketing','india'),
(8,'rakesh',100000,'IT','us'),
(9,'aditya',65000,'IT','india'),
(10,'rahul',50000,'marketing','us')]

emp_schema = ['id','name','salary','dept','country']

emp_df=spark.createDataFrame(emp_data,emp_schema)

emp_df.show()

# COMMAND ----------

emp_df.groupBy('dept','country')\
    .agg(sum('salary')).sort('dept','country').show()

# COMMAND ----------

emp_df.createOrReplaceTempView('tbl')

# COMMAND ----------

spark.sql(""" select dept, SUM(salary) from tbl group by dept """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Join

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']

customer_df=spark.createDataFrame(customer_data,customer_schema)

customer_df.createOrReplaceTempView('cust_tbl')

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema=['customer_id','product_id','quantity','date_of_purchase']

sales_df=spark.createDataFrame(sales_data,sales_schema)

sales_df.createOrReplaceTempView('sales_tbl')

product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']

product_df=spark.createDataFrame(product_data,product_schema)

product_df.createOrReplaceTempView('pdt_tbl')

# COMMAND ----------

customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'inner').show()

# COMMAND ----------

customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'inner').select('customer_id').show()

# COMMAND ----------

customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'inner').select(sales_df['customer_id']).show()

# COMMAND ----------

 customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'left').show()

# COMMAND ----------

sales_df.join(product_df,sales_df['product_id']==product_df['id'],'right').show()

# COMMAND ----------

 customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'outer').show()

# COMMAND ----------

 customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'left_semi').show()

# COMMAND ----------

 customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'left_anti').show()

# COMMAND ----------

customer_df.crossJoin(sales_df).show()

# COMMAND ----------

customer_df.crossJoin(sales_df).count()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema=['id','name','salary','dept','gender']

emp_df=spark.createDataFrame(emp_data,emp_schema)

# COMMAND ----------

# GROUP BY

emp_df.groupBy('dept').agg(sum('salary')).show()

# COMMAND ----------

from pyspark.sql.window import Window
window = Window.partitionBy('dept')
emp_df.withColumn('Total_salary',sum(col('salary')).over(window)).show()

# COMMAND ----------

from pyspark.sql.window import Window
window = Window.partitionBy('dept').orderBy('salary')
emp_df.withColumn('row_number',row_number().over(window))\
    .withColumn('Rank',rank().over(window))\
        .withColumn('DenseRank',dense_rank().over(window))\
    .show()

# COMMAND ----------

from pyspark.sql.window import Window
window = Window.partitionBy('dept','gender').orderBy('salary')
emp_df.withColumn('row_number',row_number().over(window))\
    .withColumn('Rank',rank().over(window))\
        .withColumn('DenseRank',dense_rank().over(window))\
    .show()

# COMMAND ----------

from pyspark.sql.window import Window
window = Window.partitionBy('dept').orderBy(desc('salary'))
emp_df.withColumn('row_number',row_number().over(window))\
    .withColumn('Rank',rank().over(window))\
        .withColumn('DenseRank',dense_rank().over(window))\
            .filter(col('DenseRank')<=2)\
    .show()

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=['product_id','product_name','sales_date','sales']

product_df=spark.createDataFrame(product_data,product_schema)

# COMMAND ----------

window=Window.partitionBy('product_id').orderBy('sales_date')
last_month_df= product_df.withColumn('prev_month_sale',lag(col('sales'),1).over(window))
last_month_df.show()

# COMMAND ----------

last_month_df.withColumn('per_loss_gain',round(((col("sales")-col('prev_month_sale'))/col('sales'))*100,2)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Range and Row between / first and last

# COMMAND ----------

# MAGIC %md
# MAGIC Difference in First and last Sale

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

product_data = [
(2,"samsung","01-01-1995",11000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-01-2006",15000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(3,"oneplus","01-01-2010",23000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

product_df.show()

# COMMAND ----------

window = Window.partitionBy('product_id').orderBy('sales_date')

# COMMAND ----------

product_df.withColumn('first_sale',first('sales').over(window))\
    .withColumn('last_sale',last('sales').over(window))\
        .show()

# COMMAND ----------

product_df.withColumn('first_sale',first('sales').over(window))\
    .withColumn('last_sale',last('sales').over(window))\
        .explain()

# COMMAND ----------

window = Window.partitionBy('product_id').orderBy('sales_date')\
    .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

# COMMAND ----------

product_df.withColumn('first_sale',first('sales').over(window))\
    .withColumn('last_sale',last('sales').over(window))\
        .show()

# COMMAND ----------

product_df.withColumn('first_sale',first('sales').over(window))\
    .withColumn('last_sale',last('sales').over(window))\
        .withColumn('diff',col('last_sale')-col('first_sale'))\
            .select('product_id','product_name','diff').distinct()\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Employee not completing 8 hours in office

# COMMAND ----------

emp_data = [(1,"manish","11-07-2023","10:20"),
        (1,"manish","11-07-2023","11:20"),
        (2,"rajesh","11-07-2023","11:20"),
        (1,"manish","11-07-2023","11:50"),
        (2,"rajesh","11-07-2023","13:20"),
        (1,"manish","11-07-2023","19:20"),
        (2,"rajesh","11-07-2023","17:20"),
        (1,"manish","12-07-2023","10:32"),
        (1,"manish","12-07-2023","12:20"),
        (3,"vikash","12-07-2023","09:12"),
        (1,"manish","12-07-2023","16:23"),
        (3,"vikash","12-07-2023","18:08")]

emp_schema = ["id", "name", "date", "time"]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

emp_df.show()

# COMMAND ----------

emp_df = emp_df.withColumn('timestamp',
                           from_unixtime(unix_timestamp(expr('CONCAT(date," ",time)'),'dd-MM-yyyy HH:mm')))

# COMMAND ----------

emp_df.show()

# COMMAND ----------

window = Window.partitionBy('id','date').orderBy('date')\
    .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

# COMMAND ----------

emp_df.withColumn('login',first('timestamp').over(window))\
    .withColumn('logout',last('timestamp').over(window)).show()

# COMMAND ----------

emp_df.withColumn('login',first('timestamp').over(window))\
    .withColumn('logout',last('timestamp').over(window)).printSchema()

# COMMAND ----------

emp_df.withColumn('login',first('timestamp').over(window))\
    .withColumn('logout',last('timestamp').over(window))\
        .withColumn('login',to_timestamp('login','yyyy-MM-dd HH:mm:ss'))\
            .withColumn('logout',to_timestamp('logout','yyyy-MM-dd HH:mm:ss'))\
                .withColumn('total_time',col('logout')-col('login')).show(truncate=False)

# COMMAND ----------

emp_df.withColumn('login',first('timestamp').over(window))\
    .withColumn('logout',last('timestamp').over(window))\
        .withColumn('login',to_timestamp('login','yyyy-MM-dd HH:mm:ss'))\
            .withColumn('logout',to_timestamp('logout','yyyy-MM-dd HH:mm:ss'))\
                .withColumn('total_time',col('logout')-col('login'))\
                    .select('id','name',"date",'total_time').distinct().show()

# COMMAND ----------

emp_df=emp_df.withColumn('login',first('timestamp').over(window))\
    .withColumn('logout',last('timestamp').over(window))\
        .withColumn('login',to_timestamp('login','yyyy-MM-dd HH:mm:ss'))\
            .withColumn('logout',to_timestamp('logout','yyyy-MM-dd HH:mm:ss'))\
                .withColumn('total_time',col('logout')-col('login'))\
                .select('id','name',"date",'total_time').distinct()

# COMMAND ----------

emp_df.createOrReplaceTempView('tbl')
#emp_df = emp_df.withColumn('in_hrs',expr("extract(epoch from total_time) / 3600"))

# COMMAND ----------

emp_df=spark.sql("""
          select id , name, date ,extract(hour from total_time) as hours from tbl """)

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

emp_df = emp_df.withColumn('hours',col('hours').cast('int'))

# COMMAND ----------

emp_df.filter(col('hours')<8).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Running sales/cumulative sales

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

# COMMAND ----------

product_df.show()

# COMMAND ----------

window= Window.partitionBy('product_id').orderBy('sales_date').rowsBetween(-2,0)

# COMMAND ----------

product_df = product_df.withColumn('running_sum',sum('sales').over(window))
product_df.show()

# COMMAND ----------

window2= Window.partitionBy('product_id').orderBy('sales_date')
product_df = product_df.withColumn('row_number',row_number().over(window2))
product_df.show()

# COMMAND ----------

product_df = product_df.filter(col('row_number')>2)

# COMMAND ----------

df = product_df.withColumn('Avg_sale',round(col('running_sum')/3,2)).select('product_id','product_name','sales_date','Avg_sale')

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Flatten json
# MAGIC

# COMMAND ----------

restaurant_json_data = spark.read.format('json')\
  .option('multiline','true')\
    .option('inferschema','true')\
      .load('/FileStore/tables/resturant_json_data.json')

# COMMAND ----------

ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

restaurant_json_data.show()

# COMMAND ----------

restaurant_json_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

restaurant_json_data.select('*',explode('restaurants').alias('new_restaurants')).drop('restaurants').printSchema()

# COMMAND ----------

restaurant_json_data.select('*',explode('restaurants').alias('new_restaurants')).drop('restaurants')\
.select('new_restaurants.restaurant.name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC SCD-2

# COMMAND ----------

customer_dim_data = [

(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None),
]

customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

customer_dim_df = spark.createDataFrame(data= customer_dim_data,schema=customer_schema)

sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Join the DF to identify changes in address

# COMMAND ----------

joined_data = customer_dim_df.join(sales_df,customer_dim_df['id']==sales_df['customer_id'],'left')

# COMMAND ----------

display(joined_data)

# COMMAND ----------

new_record_df = joined_data.where(
    (col('food_delivery_address') != col('city')) & (col('active') == 'Y'))\
    .withColumn('active',lit('Y'))\
    .withColumn('effective_start_date',col('sales_date'))\
    .withColumn('effective_end_date',lit(None))\
        .select(
            'customer_id',
            'customer_name',
            col('food_delivery_address').alias('city'),
            'food_delivery_country',
            'active',
            'effective_start_date',
            'effective_end_date'   
)
new_record_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Update Old record

# COMMAND ----------

old_record_df = joined_data.where(
    (col('food_delivery_address') != col('city')) & (col('active') == 'Y'))\
    .withColumn('active',lit('N'))\
    .withColumn('effective_end_date',col('sales_date'))\
        .select(
            'customer_id',
            'customer_name',
            'city',
            'food_delivery_country',
            'active',
            'effective_start_date',
            'effective_end_date'   
)
old_record_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Find out new customers and insert them

# COMMAND ----------

new_customers = sales_df.join(
  customer_dim_df,sales_df['customer_id']==customer_dim_df['id'],'leftanti'
)\
    .withColumn('active',lit('Y'))\
    .withColumn('effective_start_date',col('sales_date'))\
    .withColumn('effective_end_date',lit(None))\
        .select(
            'customer_id',
            'customer_name',
            'food_delivery_address',
            'food_delivery_country',
            'active',
            'effective_start_date',
            'effective_end_date'   
)
new_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Merge all records in one DF

# COMMAND ----------

final_records = customer_dim_df.union(new_record_df).union(old_record_df).union(new_customers
)
final_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove the duplicate data

# COMMAND ----------

window = Window.partitionBy('id','active').orderBy(col('effective_start_date').desc())

final_records.withColumn('rnk',rank().over(window))\
  .filter(~((col('active')=='Y')&(col('rnk')>=2))).drop('rnk').show()

# COMMAND ----------


