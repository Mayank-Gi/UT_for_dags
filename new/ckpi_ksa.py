import sys
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from delta import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col,upper,trim,broadcast,sum, count, round , avg, current_date, when
from pyspark.sql.types import *
from cvmdatalake import conformed, creat_delta_table_if_not_exists, get_s3_path


def monteary_brand_add(bu_df, sponsor: str, months_ago , alias , main_df) -> DataFrame:
    ''' function to calculated total tranaction for a customer
      for a brand in a defined time period '''
    filtered_trx = (
    bu_df
    .filter((col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
            & (col(conformed.Transactions.dat_date_type_1.name) >= months_ago)) \
    .groupBy(conformed.Transactions.idi_counterparty_gr.name) \
    .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(alias)))
    main_df = main_df.join(filtered_trx, conformed.Transactions.idi_counterparty_gr.name,'left')
    return main_df

def total_transactions_lifetime(bu_df, sponsor: str , alias , main_df) -> DataFrame:
    ''' function to calculate total number of transaction
      for a customer in a brand in life time period'''
    filtered_trx = (
        bu_df
        .filter(
            (col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
        & (col(conformed.Transactions.cod_sor_gr.name) == '971002'))\
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)\
        .agg(count(conformed.Transactions.idi_turnover.name).alias(alias)))

    main_df = main_df.join(filtered_trx, conformed.Transactions.idi_counterparty_gr.name,'left')
    return main_df

def add_active_inactive_flag(bu_df, sponsor: str, months_ago, alias1, alias2, main_df) -> DataFrame:
    ''' function to add active and inactive status of a customer in a brand'''
    filtered_trx = (
        bu_df
        .filter(
        (col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
        &(col(conformed.Transactions.cod_sor_gr.name) == '971002'))
        .select("dat_date_type_1","idi_counterparty_gr")) \
        .groupBy(conformed.Transactions.idi_counterparty_gr.name) \
        .agg(max(conformed.Transactions.dat_date_type_1.name).alias("max_date"))

    filtered_trx = filtered_trx.withColumn(alias1,
                                        F.when(F.col("max_date") >= months_ago, 1).otherwise(None))
    filtered_trx = filtered_trx.withColumn(alias2, F.when(F.col(alias1) == 1, None).otherwise(1))
    filtered_trx = filtered_trx.drop("max_date")
    main_df = main_df.join(filtered_trx, conformed.Transactions.idi_counterparty_gr.name, 'left')
    return main_df


def average_customer_spend_brand_add(bu_df, sponsor: str, months_ago ,
                                      alias , main_df) -> DataFrame:
    ''' function to calculate average spend of a customer for a life style brand '''
    filtered_trx = (
        bu_df
        .filter(
        (col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
        & (col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        & (col(conformed.Transactions.cod_sor_gr.name) == '971002')) \
        .groupBy(conformed.Transactions.idi_counterparty_gr.name) \
        .agg(avg(conformed.Transactions.cua_amount_type_1.name).alias(alias))
        )
    main_df = main_df.join(filtered_trx, conformed.Transactions.idi_counterparty_gr.name,'left')
    return main_df



def avg_transaction_value_brand_add(bu_df, sponsor: str, months_ago , alias , main_df) -> DataFrame:
    ''' fucntion to calculate average transction value for a perticular brand'''
    filtered_trx = (
        bu_df
        .filter(
            (col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
        & (col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        & (col(conformed.Transactions.cod_sor_gr.name) == '971002')) \
        .groupBy((conformed.Transactions.idi_counterparty_gr.name)) \
        .agg(avg(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )

    main_df = main_df.join(filtered_trx, conformed.Transactions.idi_counterparty_gr.name,'left')
    return main_df




############# main code #############################################

args = getResolvedOptions(sys.argv, ['cvm_environment'])
environment = args['cvm_environment']
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
cvm_environment = args['cvm_environment']


# defining transaction dataframe consisting all BU's, with KSA region only and removing future dates
conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)
trx = DeltaTable.forPath(spark, conformed_trans_path).toDF()
# trx.show()
trx=trx.filter(
                (col(conformed.Transactions.des_country_name.name) == 'KSA')
               & (col(conformed.Transactions.dat_date_type_1.name) <= current_date())
               ).select("idi_counterparty_gr","dat_date_type_1","cua_amount_type_1","idi_turnover","bu","cod_sor_gr","des_country_name","des_maf_brand_name")
# trx.show()


# created trx_fsn df for lifestyle transactions
trx_fsn = trx.filter((col(conformed.Transactions.bu.name) == 'fsn_new')
& (col(conformed.Transactions.cod_sor_gr.name) == '971002')).cache()

# defining flowing dataframe
Flowing_df=trx_fsn.select(conformed.Transactions.idi_counterparty_gr.name).cache()
Flowing_df.show()

# calculated max and min date to futher define required time periods dates
df_till_date = trx_fsn.select(min(trx.dat_date_type_1) ).alias("till_date")
df_max = trx_fsn.select(max(trx.dat_date_type_1)).alias("latest_months")

latest_date = df_max.collect()[0][0]
print(latest_date)
max_date = latest_date

one_day_ago = (max_date - timedelta(days=1))
seven_days_ago = (max_date - timedelta(days=7))
current_month = latest_date.month
current_year = latest_date.year
one_month_ago = (max_date - relativedelta(months=1))
two_months_ago = (max_date - relativedelta(months=2))
previous_month = one_month_ago.month
three_months_ago =  (max_date - relativedelta(months=3))
six_months_ago =  (max_date - relativedelta(months=6))
nine_months_ago =  (max_date - relativedelta(months=9))
twelve_months_ago =  (max_date - relativedelta(months=12))
previous_year = twelve_months_ago.year
twentyfour_months_ago =  (max_date - relativedelta(months=24))
till_date = df_till_date.collect()[0][0]

print(f"one_month_ago = {one_month_ago}")
print(f"three_months_ago = {three_months_ago}")
print(f"six_months_ago = {six_months_ago}")
print(f"nine_months_ago = {nine_months_ago}")
print(f"seven_days_ago = {seven_days_ago}")
print(f"one_day_ago = {one_day_ago}")
print(f"till_date = {till_date}")


print("latest_date: ", latest_date)
date_format = 'yyyy-MM-dd'

print("current_month: ", current_month)
print("current_year: ", current_year)

if current_month == 10 or current_month == 11 or current_month == 12:
    current_month_start_date = f'{current_year}-{current_month}-01'

else:
    current_month_start_date = f'{current_year}-0{current_month}-01'

Flowing_df = average_customer_spend_brand_add(trx_fsn,"THAT (Store)", six_months_ago,
 "average_customer_spend_lifestyle_that_last_6_months_ksa", Flowing_df)
Flowing_df.show()
Flowing_df = average_customer_spend_brand_add(trx_fsn,"LuluLemon (Store)", six_months_ago,
 "average_customer_spend_lifestyle_lll_last_6_months_ksa", Flowing_df)
Flowing_df.show()
Flowing_df = average_customer_spend_brand_add(trx_fsn,"All Saints (Store)", six_months_ago,
 "average_customer_spend_lifestyle_als_last_6_months_ksa", Flowing_df)
Flowing_df.show()
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Hollister (Store)", six_months_ago,
 "average_customer_spend_lifestyle_hs_last_6_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", six_months_ago,
 "average_customer_spend_lifestyle_anb_last_6_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Crate & Barrel (Store)", six_months_ago,
 "average_customer_spend_lifestyle_cnb_last_6_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"CB2 (Store)", six_months_ago,
 "average_customer_spend_lifestyle_cb2_last_6_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Fashion For Less (Store)", six_months_ago,
 "average_customer_spend_lifestyle_ffl_last_6_months_ksa", Flowing_df)

Flowing_df = average_customer_spend_brand_add(trx_fsn,"THAT (Store)", three_months_ago,
 "average_customer_spend_lifestyle_that_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"LuluLemon (Store)", three_months_ago,
 "average_customer_spend_lifestyle_lll_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"All Saints (Store)", three_months_ago,
 "average_customer_spend_lifestyle_als_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Hollister (Store)", three_months_ago,
 "average_customer_spend_lifestyle_hs_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", three_months_ago,
 "average_customer_spend_lifestyle_anb_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Crate & Barrel (Store)", three_months_ago,
 "average_customer_spend_lifestyle_cnb_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"CB2 (Store)", three_months_ago,
 "average_customer_spend_lifestyle_cb2_last_3_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Fashion For Less (Store)", three_months_ago,
 "ffl_average_customer_spend_lifestyle_ffl_last_3_months_ksa", Flowing_df)

Flowing_df = average_customer_spend_brand_add(trx_fsn,"THAT (Store)", one_month_ago,
 "average_customer_spend_lifestyle_that_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"LuluLemon (Store)", one_month_ago,
 "average_customer_spend_lifestyle_lll_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"All Saints (Store)", one_month_ago,
 "average_customer_spend_lifestyle_als_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Hollister (Store)", one_month_ago,
 "average_customer_spend_lifestyle_hs_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", one_month_ago,
 "average_customer_spend_lifestyle_anb_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Crate & Barrel (Store)", one_month_ago,
 "average_customer_spend_lifestyle_cnb_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"CB2 (Store)", one_month_ago,
 "average_customer_spend_lifestyle_cb2_last_1_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Fashion For Less (Store)", one_month_ago,
 "ffl_average_customer_spend_lifestyle_ffl_last_1_months_ksa", Flowing_df)

Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"THAT (Store)", two_months_ago,
 "average_customer_spend_lifestyle_that_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"LuluLemon (Store)", two_months_ago,
 "average_customer_spend_lifestyle_lll_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"All Saints (Store)", two_months_ago,
 "average_customer_spend_lifestyle_als_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"Hollister (Store)", two_months_ago,
 "average_customer_spend_lifestyle_hs_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", two_months_ago,
 "average_customer_spend_lifestyle_anb_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"Crate & Barrel (Store)", two_months_ago,
 "average_customer_spend_lifestyle_cnb_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"CB2 (Store)", two_months_ago,
 "average_customer_spend_lifestyle_cb2_last_2_months_ksa", Flowing_df)
Flowing_df = Flowing_df = average_customer_spend_brand_add(trx_fsn,"Fashion For Less (Store)", two_months_ago,
 "ffl_average_customer_spend_lifestyle_ffl_last_2_months_ksa", Flowing_df)

Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"THAT (Store)", three_months_ago,
 "total_spend_lifestyle_that_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"LuluLemon (Store)", three_months_ago,
 "total_spend_lifestyle_lll_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"All Saints (Store)", three_months_ago,
 "total_spend_lifestyle_als_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"Hollister (Store)", three_months_ago,
 "total_spend_lifestyle_hs_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", three_months_ago,
 "total_spend_lifestyle_anb_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"Crate & Barrel (Store)", three_months_ago,
 "total_spend_lifestyle_cnb_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"CB2 (Store)", three_months_ago,
 "total_spend_lifestyle_cb2_last_3_months_ksa", Flowing_df)
Flowing_df = Flowing_df = monteary_brand_add(trx_fsn,"Fashion For Less (Store)", three_months_ago,
 "total_spend_lifestyle_ffl_last_3_months_ksa", Flowing_df)

Flowing_df = monteary_brand_add(trx_fsn,"THAT (Store)", six_months_ago,
 "total_spend_lifestyle_that_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"LuluLemon (Store)", six_months_ago,
 "total_spend_lifestyle_lll_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"All Saints (Store)", six_months_ago,
 "total_spend_lifestyle_als_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Hollister (Store)", six_months_ago,
 "total_spend_lifestyle_hs_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", six_months_ago,
 "total_spend_lifestyle_anb_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Crate & Barrel (Store)", six_months_ago,
 "total_spend_lifestyle_cnb_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"CB2 (Store)", six_months_ago,
 "total_spend_lifestyle_cb2_last_6_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Fashion For Less (Store)", six_months_ago,
 "total_spend_lifestyle_ffl_last_6_months_ksa", Flowing_df)

Flowing_df = monteary_brand_add(trx_fsn,"THAT (Store)", twelve_months_ago,
 "total_spend_lifestyle_that_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"LuluLemon (Store)", twelve_months_ago,
 "total_spend_lifestyle_lll_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"All Saints (Store)", twelve_months_ago,
 "total_spend_lifestyle_als_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Hollister (Store)", twelve_months_ago,
 "total_spend_lifestyle_hs_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", twelve_months_ago,
 "total_spend_lifestyle_anb_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Crate & Barrel (Store)", twelve_months_ago,
 "total_spend_lifestyle_cnb_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"CB2 (Store)", twelve_months_ago,
 "total_spend_lifestyle_cb2_last_12_months_ksa", Flowing_df)
Flowing_df = monteary_brand_add(trx_fsn,"Fashion For Less (Store)", twelve_months_ago,
 "total_spend_lifestyle_ffl_last_12_months_ksa", Flowing_df)

Flowing_df = total_transactions_lifetime(trx_fsn,"THAT (Store)",
 "total_transactions_lifestyle_that_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"LuluLemon (Store)",
 "total_transactions_lifestyle_lll_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"All Saints (Store)",
"total_transactions_lifestyle_als_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"Hollister (Store)",
 "total_transactions_lifestyle_hs_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"Abercrombie and Fitch (Store)",
 "total_transactions_lifestyle_anb_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"Crate & Barrel (Store)",
 "total_transactions_lifestyle_cnb_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"CB2 (Store)",
 "total_transactions_lifestyle_cb2_lifetime_ksa", Flowing_df)
Flowing_df = total_transactions_lifetime(trx_fsn,"Fashion For Less (Store)",
 "total_transactions_lifestyle_ffl_lifetime_ksa", Flowing_df)


Flowing_df = average_customer_spend_brand_add(trx_fsn,"THAT (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_that_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"LuluLemon (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_lll_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"All Saints (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_als_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Hollister (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_hs_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Abercrombie and Fitch (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_anb_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Crate & Barrel (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_cnb_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"CB2 (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_cb2_last_12_months_ksa", Flowing_df)
Flowing_df = average_customer_spend_brand_add(trx_fsn,"Fashion For Less (Store)", twelve_months_ago,
 "average_transaction_value_lifestyle_ffl_last_12_months_ksa", Flowing_df)

Flowing_df = add_active_inactive_flag(trx_fsn,"THAT", six_months_ago,
 "lifestyle_that_status_active_ksa","lifestyle_that_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"LuluLemon (Store)", six_months_ago,
 "lifestyle_lll_status_active_ksa","lifestyle_lll_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"All Saints (Store)", six_months_ago,
 "lifestyle_als_status_active_ksa","lifestyle_als_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"Hollister (Store)", six_months_ago,
 "lifestyle_hs_status_active_ksa","lifestyle_hs_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"Abercrombie and Fitch (Store)", six_months_ago,
 "lifestyle_anb_status_active_ksa","lifestyle_anb_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"Crate & Barrel (Store)", six_months_ago,
 "lifestyle_cnb_status_active_ksa","lifestyle_cnb_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"CB2 (Store)", six_months_ago,
 "lifestyle_cb2_status_active_ksa","lifestyle_cb2_status_inactive_ksa", Flowing_df)
Flowing_df = add_active_inactive_flag(trx_fsn,"Fashion For Less (Store)", six_months_ago,
 "lifestyle_ffl_status_active_ksa","lifestyle_ffl_status_inactive_ksa", Flowing_df)


print('XXXXXXXXXXXXXXXXXXXX_end', Flowing_df.printSchema())

rounded_columns = [
col_name for col_name, col_type in Flowing_df.dtypes if col_type == "double" or col_type == "float"
]
print('XXXXXXXXXXXXXXXXXXXX___rounded_columns', rounded_columns)

expressions = [col(col_name) if col_name not in rounded_columns else round(when(col(col_name).isNotNull(),
 col(col_name)), 2).alias(col_name) for col_name in Flowing_df.columns]
print('XXXXXXXXXXXXXXXXXXXX___expressions', expressions)

Flowing_df = Flowing_df.select(*expressions)

print('XXXXXXXXXXXXXXXXXXXX___final_df', Flowing_df.printSchema())
Flowing_df.show()
Flowing_df.explain()

# joining it with bu external id to map columns with external id
conformed_bu_external_id_path = get_s3_path(conformed.BuExternalId, lake_descriptor)
bu_external = DeltaTable.forPath(spark, conformed_bu_external_id_path).toDF()
bu_external=bu_external.filter(
            (col("org_code") == 'FSN')
            ) \
            .select('gcr_id','external_id').distinct()

Flowing_df_ext_id = Flowing_df.join(
    bu_external,
    Flowing_df["idi_counterparty_gr"] == bu_external["gcr_id"],
    how="inner")

Flowing_df_ext_id = Flowing_df_ext_id.drop("idi_counterparty_gr").withColumnRenamed(
    "external_id", "idi_counterparty_gr")
Flowing_df_ext_id = Flowing_df_ext_id.dropDuplicates(["idi_counterparty_gr"])
Flowing_df_ext_id.show()
num_partitions = Flowing_df_ext_id.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")
# creat_delta_table_if_not_exists(spark, conformed.ProfileCalculatedKpi, lake_descriptor)
# kpi_path = get_s3_path(conformed.ProfileCalculatedKpi, lake_descriptor)
# s3://cvm-uat-conformed-d5b175d/calculated_kpi_ksa/
# Flowing_df.write.format('delta').mode('overwrite').option("overwriteSchema", "True").save("s3://cvm-uat-conformed-d5b175d/calculated_kpi_ksa/")
# Flowing_df_ext_id.write.format('delta').mode('overwrite').option("overwriteSchema", "True").save("s3://cvm-uat-conformed-d5b175d/calculated_kpi_extID_ksa/")

