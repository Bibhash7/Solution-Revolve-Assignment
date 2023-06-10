import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import explode



def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Users/bibha/PycharmProjects/Ass2/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Users/bibha/PycharmProjects/Ass2/input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/bibha/PycharmProjects/Ass2/input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="C:/Users/bibha/PycharmProjects/Ass2/output_data/outputs/")
    return vars(parser.parse_args())


def createNewSession(sessionname):
    return SparkSession.builder.appName(sessionname).getOrCreate()
def main():
    params = get_params()
    #print(params)
    spark = createNewSession(sessionname='assignment')
    df_customer = spark.read.format('csv').option('inferSchema', 'true').option('header', 'true').option('path',params['customers_location']).load()
    df_products = spark.read.format('csv').option('inferSchema', 'true').option('header', 'true').option('path', params['products_location']).load()
    #print(df_customer.head(5))
    df_json = spark.read.format('org.apache.spark.sql.json').load(params['transactions_location'])
    df_filtered = df_json.select(F.col('customer_id'),F.col("basket.product_id"))
    df_exploded = df_filtered.select(F.col('customer_id'),explode(df_filtered.product_id))
    df_req = df_exploded.withColumnRenamed('col','product_id')
    df_grouped = df_req.groupBy('customer_id','product_id').count().withColumnRenamed('count','purchase_count')
    df_grouped.createOrReplaceTempView("df_grouped_view")

    df_customer.createOrReplaceTempView("df_loyal_view")
    df_semi = spark.sql("""SELECT C.customer_id,L.loyalty_score,C.product_id,C.purchase_count
                        FROM df_grouped_view C 
                        INNER JOIN df_loyal_view L
                        ON C.customer_id = L.customer_id
                     """)
    df_semi.createOrReplaceTempView("df_semi")
    df_products.createOrReplaceTempView("df_products")
    df_final = spark.sql("""SELECT C.customer_id,C.loyalty_score,C.product_id, P.product_category, C.purchase_count
                            FROM df_semi C 
                            INNER JOIN df_products P
                            ON C.product_id = P.product_id
                         """)
    print(df_final.show())
    df_final.coalesce(1).write.format('json').save(params['output_location'])








    

if __name__ == "__main__":
    main()
