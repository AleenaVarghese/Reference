import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, from_json, size, split, when, coalesce, lit, to_timestamp, to_date
from pyspark.sql.types import IntegerType, BooleanType, FloatType
import json
import ast


spark = SparkSession.builder.appName(“Pool_Datamart”).enableHiveSupport().getOrCreate()

spark.conf.set(“spark.sql.adaptive.enabled”, True)
spark.conf.set(“spark.sql.broadcastTimeout”, 600)
spark.conf.set(“spark.sql.autoBroadcastJoinThreshold”, 52428800)
spark.conf.set(‘spark.sql.legacy.timeParserPolicy’, ‘LEGACY’)


bucket_name = sys.argv[1]
pool_mongo_db = sys.argv[2]
sql_files = sys.argv[3]
external_table_s3_path = bucket_name + ‘products_datamart/pool_transactions_view’
sqls_path = sql_files+“transactions_view_sqls.txt”

print(‘Pool Transactions Data Refresh Started’)

def list_sum(x):
    if x is not None and x.strip(‘/[/]/“‘) != ‘’:
        x = [float(i.strip(‘/[/]/“‘)) for i in ast.literal_eval(json.dumps(x, ensure_ascii=False)).split(‘,’) if i is not None and i.strip(‘/[/]/“‘) != ‘’ and i.isspace() == False]
        return(sum(x))
    else:
        return(0)

func_list_sum = udf(list_sum, FloatType())

sqls = spark.read.option(“lineSep”, “;”).text(sqls_path)


transactions_sql = sqls.collect()[0][0]
transactions = spark.sql(transactions_sql.format(pool_mongo_db)) \
    .withColumn(‘lean_transaction’, coalesce(col(‘lean_transaction’), lit(‘false’))) \
    .withColumn(‘fulfillment’, coalesce(col(‘fulfillment’), lit(‘0’).cast(BooleanType()))) \
    .withColumn(‘mcd_count’, when(size(split(col(‘mcd_id’), ‘,’)) == -1, 0).otherwise(size(split(col(‘mcd_id’), ‘,’)))) \
    .withColumn(‘pool_audited’, col(‘auditor_id’).isNotNull()) \
    .withColumn(‘cashflow_accepted’, when(col(‘type’) == ‘Ptc::Transaction’,func_list_sum(‘cashflow_accepted_interest_ptc’) + func_list_sum(‘cashflow_accepted_principal_ptc’)) \
                                    .when(col(‘type’) == ‘Assignment::Transaction’, col(‘cashflow_accepted_dav2’)) \
                                    .when(col(‘type’) == ‘DirectAssignment::Transaction’, col(‘cashflow_accepted_dav1’)) \
                                    .otherwise(None)) \
    .withColumn(‘principal_rejected’,when((col(‘type’) == ‘Ptc::Transaction’) | (col(‘type’) == ‘Assignment::Transaction’),func_list_sum(‘principal_rejected’)) \
                                    .otherwise(col(‘principal_rejected’))) \
    .withColumn(‘exp_interests_count’, when(size(split(col(‘exp_interests’), ‘,’)) == -1, 0).otherwise(size(split(col(‘exp_interests’), ‘,’))))

transactions_ptc = transactions.filter(col(‘type’).isin(‘Securitization’, ‘Ptc::Transaction’)) \
                            .withColumn(‘a1_investor_id’, split(col(‘investor_id’), ‘,’)[0]) \
                            .withColumn(‘a2_investor_id’, split(col(‘investor_id’), ‘,’)[1]) \
                            .withColumn(‘a3_investor_id’, split(col(‘investor_id’), ‘,’)[2]) \
                            .withColumn(‘a4_investor_id’, split(col(‘investor_id’), ‘,’)[3]) \
                            .withColumn(‘a1_investor_value’, split(col(‘investor_value’), ‘,’)[0]) \
                            .withColumn(‘a2_investor_value’, split(col(‘investor_value’), ‘,’)[1]) \
                            .withColumn(‘a3_investor_value’, split(col(‘investor_value’), ‘,’)[2]) \
                            .withColumn(‘a4_investor_value’, split(col(‘investor_value’), ‘,’)[3]) \
                            .withColumn(‘a1_investor_rating’, split(col(‘investor_rating’), ‘,’)[0]) \
                            .withColumn(‘a2_investor_rating’, split(col(‘investor_rating’), ‘,’)[1]) \
                            .withColumn(‘a3_investor_rating’, split(col(‘investor_rating’), ‘,’)[2]) \
                            .withColumn(‘a4_investor_rating’, split(col(‘investor_rating’), ‘,’)[3]) \
                            .withColumn(‘a1_investor_unit’, split(col(‘investor_unit’), ‘,’)[0]) \
                            .withColumn(‘a2_investor_unit’, split(col(‘investor_unit’), ‘,’)[1]) \
                            .withColumn(‘a3_investor_unit’, split(col(‘investor_unit’), ‘,’)[2]) \
                            .withColumn(‘a4_investor_unit’, split(col(‘investor_unit’), ‘,’)[3])

transactions_ptc = transactions_ptc.withColumn(‘a1_investor_amount’, (col(‘a1_investor_value’)/100)*col(‘amount’)) \
                                   .withColumn(‘a2_investor_amount’, (col(‘a2_investor_value’)/100)*col(‘amount’)) \
                                   .withColumn(‘a3_investor_amount’, (col(‘a3_investor_value’)/100)*col(‘amount’)) \
                                   .withColumn(‘a4_investor_amount’, (col(‘a4_investor_value’)/100)*col(‘amount’))



entity_sql = sqls.collect()[1][0]

entities = spark.sql(entity_sql.format(pool_mongo_db)).cache()

transactions_ptc = transactions_ptc.join(entities.alias(‘a1’), col(‘a1_investor_id’) == col(‘a1.entity_id’), ‘left’) \
                                .join(entities.alias(‘a2’), col(‘a2_investor_id’) == col(‘a2.entity_id’), ‘left’) \
                                .join(entities.alias(‘a3’), col(‘a3_investor_id’) == col(‘a3.entity_id’), ‘left’) \
                                .join(entities.alias(‘a4’), col(‘a4_investor_id’) == col(‘a4.entity_id’), ‘left’) \
                                .join(entities.alias(‘ra’), col(‘rating_agency_id’) == col(‘ra.entity_id’), ‘left’) \
                                .join(entities.alias(‘lf’), col(‘law_firm_id’) == col(‘lf.entity_id’), ‘left’) \
                                .join(entities.alias(‘au’), col(‘auditor_id’) == col(‘au.entity_id’), ‘left’) \
                                .withColumn(‘a1_investor_name’, col(‘a1.entity_name’)) \
                                .withColumn(‘a2_investor_name’, col(‘a2.entity_name’)) \
                                .withColumn(‘a3_investor_name’, col(‘a3.entity_name’)) \
                                .withColumn(‘a4_investor_name’, col(‘a4.entity_name’)) \
                                .withColumn(‘rating_agency_name’, col(‘ra.entity_name’)) \
                                .withColumn(‘law_firm_name’, col(‘lf.entity_name’)) \
                                .withColumn(‘auditor_name’, col(‘au.entity_name’)) \
                                .select(‘id’, ‘slug_id’, ‘type’, ‘lean_transaction’, ‘mcd_id’,‘mcd_count’, ‘customer_id’, \
                                    ‘fulfillment’, ‘deal_name’, ‘created_date’, ‘created_by’, ‘client_name’, \
                                    ‘rating’, ‘amount’, ‘transaction_state’, ‘asset_class’, ‘tenor_months’, ‘updated_at’, ‘pool_original’, \
                                    ‘pool_accepted’, ‘pool_rejected’, ‘pool_audited’, ‘principal_original’, ‘principal_rejected’, ‘principal_accepted’, \
                                    ‘cashflow_accepted’, ‘exp_interests’, ‘exp_interests_count’, ‘exp_interests_details’,
                                    ‘a1_investor_id’, ‘a1_investor_name’, ‘a1_investor_amount’, ‘a1_investor_value’, ‘a1_investor_unit’, ‘a1_investor_rating’, \
                                    ‘a2_investor_id’, ‘a2_investor_name’, ‘a2_investor_amount’, ‘a2_investor_value’, ‘a2_investor_unit’, ‘a2_investor_rating’, \
                                    ‘a3_investor_id’, ‘a3_investor_name’, ‘a3_investor_amount’, ‘a3_investor_value’, ‘a3_investor_unit’, ‘a3_investor_rating’, \
                                    ‘a4_investor_id’, ‘a4_investor_name’, ‘a4_investor_amount’, ‘a4_investor_value’, ‘a4_investor_unit’, ‘a4_investor_rating’, \
                                    ‘released_on’, ‘settled_on’, ‘trust_name’, ‘trustee_id’, ‘rating_agency_id’, \
                                    ‘rating_agency_name’, ‘law_firm_id’, ‘law_firm_name’, ‘auditor_id’, ‘auditor_name’, ‘trustee_fee’, \
                                    ‘rating_fee’, ‘legal_fee’, ‘audit_fee’, ‘stamp_duty’, ‘arranger_fee’, ‘maturity_ts’, ‘transaction_initiated_ts’, \
                                    ‘pool_uploaded_ts’, ‘pool_processed_ts’, ‘transaction_released_ts’, ‘express_interest_ts’, ‘structure_finalised_ts’, \
                                    ‘draft_legal_docs_ts’, ‘rating_intiated_ts’, ‘rating_rationale_ts’, ‘finalise_legal_docs_ts’, ‘transaction_finalised_ts’, \
                                    ‘trustee_tasks_ts’, ‘disburse_cp_ts’, ‘disburse_final_ts’, ‘transaction_settled_ts’, ‘auditor_actions_ts’, ‘legal_opinion_ts’, \
                                    ‘deal_called_off_ts’, ‘recommended_investor_info’, ‘yield’)

transactions = transactions.filter(col(‘type’).isin(‘Assignment::Transaction’,‘DirectAssignment::Transaction’)) \
                                    .join(entities.alias(‘ent’), col(‘investor_id’) == col(‘ent.entity_id’), ‘left’) \
                                    .join(entities.alias(‘ra’), col(‘rating_agency_id’) == col(‘ra.entity_id’), ‘left’) \
                                    .join(entities.alias(‘lf’), col(‘law_firm_id’) == col(‘lf.entity_id’), ‘left’) \
                                    .join(entities.alias(‘au’), col(‘auditor_id’) == col(‘au.entity_id’), ‘left’) \
                                    .withColumn(‘rating_agency_name’, col(‘ra.entity_name’)) \
                                    .withColumn(‘law_firm_name’, col(‘lf.entity_name’)) \
                                    .withColumn(‘auditor_name’, col(‘au.entity_name’)) \
                                    .selectExpr(‘id’, ‘slug_id’, ‘type’, ‘lean_transaction’, ‘mcd_id’, ‘mcd_count’, ‘customer_id’, \
                                    ‘fulfillment’, ‘deal_name’, ‘created_date’, ‘created_by’, ‘client_name’, \
                                    ‘rating’, ‘amount’, ‘transaction_state’, ‘asset_class’, ‘tenor_months’, ‘updated_at’, ‘pool_original’, \
                                    ‘pool_accepted’, ‘pool_rejected’,‘pool_audited’, ‘principal_original’, ‘principal_rejected’, ‘principal_accepted’, \
                                    ‘cashflow_accepted’, ‘exp_interests’, ‘exp_interests_count’, ‘exp_interests_details’, \
                                    ‘investor_id as a1_investor_id’, ‘ent.entity_name as a1_investor_name’, ‘investor_amount as a1_investor_amount’, ‘investor_value as a1_investor_value’, ‘investor_unit as a1_investor_unit’, ‘investor_rating as a1_investor_rating’, \
                                    ‘“N/A” as a2_investor_id’, ‘“N/A” as a2_investor_name’ , ‘null as a2_investor_amount’, ‘null as a2_investor_value’, ‘“N/A” as a2_investor_unit’, ‘“N/A” as a2_investor_rating’,  \
                                    ‘“N/A” as a3_investor_id’, ‘“N/A” as a3_investor_name’, ‘null as a3_investor_amount’, ‘null as a3_investor_value’, ‘“N/A” as a3_investor_unit’, ‘“N/A” as a3_investor_rating’,  \
                                    ‘“N/A” as a4_investor_id’, ‘“N/A” as a4_investor_name’, ‘null as a4_investor_amount’, ‘null as a4_investor_value’, ‘“N/A” as a4_investor_unit’, ‘“N/A” as a4_investor_rating’,  \
                                    ‘released_on’, ‘settled_on’, ‘trust_name’, ‘trustee_id’, ‘rating_agency_id’, \
                                    ‘rating_agency_name’, ‘law_firm_id’, ‘law_firm_name’, ‘auditor_id’, ‘auditor_name’, ‘trustee_fee’, \
                                    ‘rating_fee’, ‘legal_fee’, ‘audit_fee’, ‘stamp_duty’, ‘arranger_fee’, ‘maturity_ts’, ‘transaction_initiated_ts’, \
                                    ‘pool_uploaded_ts’, ‘pool_processed_ts’, ‘transaction_released_ts’, ‘express_interest_ts’, ‘structure_finalised_ts’, \
                                    ‘draft_legal_docs_ts’, ‘rating_intiated_ts’, ‘rating_rationale_ts’, ‘finalise_legal_docs_ts’, ‘transaction_finalised_ts’, \
                                    ‘trustee_tasks_ts’, ‘disburse_cp_ts’, ‘disburse_final_ts’, ‘transaction_settled_ts’, ‘auditor_actions_ts’, ‘legal_opinion_ts’, \
                                    ‘deal_called_off_ts’, ‘recommended_investor_info’, ‘yield’)


transactions = transactions.union(transactions_ptc) \
                .withColumn(‘a1_investor_amount’, col(‘a1_investor_amount’).cast(‘decimal(18,5)’)) \
                .withColumn(‘a2_investor_amount’, col(‘a2_investor_amount’).cast(‘decimal(18,5)’)) \
                .withColumn(‘a3_investor_amount’, col(‘a3_investor_amount’).cast(‘decimal(18,5)’)) \
                .withColumn(‘a4_investor_amount’, col(‘a4_investor_amount’).cast(‘decimal(18,5)’)) \
                .withColumn(‘principal_original’, col(‘principal_original’).cast(‘decimal(18,5)’)) \
                .withColumn(‘principal_accepted’, col(‘principal_accepted’).cast(‘decimal(18,5)’)) \
                .withColumn(‘cashflow_accepted’, col(‘cashflow_accepted’).cast(‘decimal(18,5)’)) \
                .withColumn(‘trustee_fee’, col(‘trustee_fee’).cast(‘decimal(18,5)’)) \
                .withColumn(‘rating_fee’, col(‘rating_fee’).cast(‘decimal(18,5)’)) \
                .withColumn(‘legal_fee’, col(‘legal_fee’).cast(‘decimal(18,5)’)) \
                .withColumn(‘audit_fee’, col(‘audit_fee’).cast(‘decimal(18,5)’)) \
                .withColumn(‘released_on’, to_date(col(‘released_on’), ‘yyyy-MM-dd’)) \
                .withColumn(‘settled_on’, to_date(col(‘settled_on’), ‘yyyy-MM-dd’)) \
                .withColumn(‘maturity_ts’, to_timestamp(col(‘maturity_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘transaction_initiated_ts’, to_timestamp(col(‘transaction_initiated_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘pool_uploaded_ts’, to_timestamp(col(‘pool_uploaded_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘pool_processed_ts’, to_timestamp(col(‘pool_processed_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘transaction_released_ts’, to_timestamp(col(‘transaction_released_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘express_interest_ts’, to_timestamp(col(‘express_interest_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘structure_finalised_ts’, to_timestamp(col(‘structure_finalised_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘draft_legal_docs_ts’, to_timestamp(col(‘draft_legal_docs_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘rating_intiated_ts’, to_timestamp(col(‘rating_intiated_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘rating_rationale_ts’, to_timestamp(col(‘rating_rationale_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘finalise_legal_docs_ts’, to_timestamp(col(‘finalise_legal_docs_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘transaction_finalised_ts’, to_timestamp(col(‘transaction_finalised_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘trustee_tasks_ts’, to_timestamp(col(‘trustee_tasks_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘disburse_cp_ts’, to_timestamp(col(‘disburse_cp_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘disburse_final_ts’, to_timestamp(col(‘disburse_final_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘transaction_settled_ts’, to_timestamp(col(‘transaction_settled_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘auditor_actions_ts’, to_timestamp(col(‘auditor_actions_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘legal_opinion_ts’, to_timestamp(col(‘legal_opinion_ts’), ‘yyyy-MM-dd HH:mm:ss’)) \
                .withColumn(‘deal_called_off_ts’, to_timestamp(col(‘deal_called_off_ts’), ‘yyyy-MM-dd HH:mm:ss’))


transactions.write.option(‘path’, external_table_s3_path).mode(‘overwrite’).saveAsTable(‘products_datamart.pool_transactions_view’)

print(‘Pool Transactions Data Refresh Completed’)