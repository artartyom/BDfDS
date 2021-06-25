import io
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
from pyspark.sql import functions as F


def process(spark, input_file, target_path):
	
	# Загрузим изначальный датасет
    df = pqt_into_spark(spark, input_file)
    
	# Уберем то, что вообще не пригодится
    df = df.drop("time", "platform", "client_union_id", "compaign_union_id")
    
    # Создадим вспомогательные таблицы
    suppTables = create_supp_tables(spark, df)
    
    # Cоберем вспомогательные таблицы в одну
    finalTable = suppTables[0] \
    .join(suppTables[1], on = "ad_id") \
    .join(suppTables[2], on = "ad_id") \
    .join(suppTables[3], on = "ad_id")
    
    # Разделим на train и test сеты
    splitData = finalTable.randomSplit([0.75, 0.25])
    
    # Запишем данные на диск
    write_split_data(splitData, target_path)

def cnt_conditional(condition):
    # для подсчета числа значений, соответствующих заданному условию
    return F.sum(F.when(condition, 1).otherwise(0))
    
def pqt_into_spark(spark, input_file):
    # Эта функция загружает заданный паркет-файл в спарк
    sparkdata = spark.read.parquet(input_file)
    return sparkdata

def create_supp_tables(spark, df):
    # Создает вспомогательные таблицы, в которых рассчитаны отдельные группировочные статистики
    # Подготовим view для SQL-запросов
    df.createOrReplaceTempView("sqlDF")
    
    # Посчитаем количество дней, которые отображалось каждое объявление
    uniqueDays = spark.sql(
    """
    SELECT ad_id, count(ad_id) as day_count
    FROM (
        SELECT ad_id, date
        FROM sqlDF
        GROUP BY ad_id, date)
    GROUP BY ad_id
    """
    )
    
    # Посчитаем CTR.
    
    CTR = df \
    .withColumn("eventView", col("event") == "view") \
    .withColumn("eventClick", col("event") == "click") \
    .groupBy("ad_id") \
    .agg(
    cnt_conditional(col("eventView")).alias("cnt_views"),
    cnt_conditional(col("eventClick")).alias("cnt_clicks")) \
    .withColumn("CTR", col("cnt_clicks") / col("cnt_views")) \
    .select("ad_id", "CTR")
    
    # Создадим dummy variables для CPC и CPM
    
    CPC_CPM = df \
    .select("ad_id", "ad_cost_type") \
    .groupBy("ad_id") \
    .agg(F.first(F.when(col("ad_cost_type") == "CPM", 1).otherwise(0)).alias("CPM"),
         F.first(F.when(col("ad_cost_type") == "CPC", 1).otherwise(0)).alias("CPC")
    )
    
    # Соберем остальные характеристики каждого объявления (уникальные)
    
    basicValues = df.select("ad_id", "target_audience_count", "has_video").distinct()
    
    return (uniqueDays, CTR, CPC_CPM, basicValues)

def write_split_data(splitData, path_out):
    # создаем zip с названием датасета и его содержимым
    zipdata = zip(("train", "test"), splitData)
    
    for i in zipdata:
        # создадим название пути для данных
        writepath = path_out + "/" + i[0]
        # Пишем датафрейм на диск как parquet-файл
        i[1].coalesce(1).write.parquet(writepath)

def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path)


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)