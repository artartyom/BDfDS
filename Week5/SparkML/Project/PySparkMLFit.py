import io
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Используйте как путь куда сохранить модель
MODEL_PATH = 'spark_ml_model'


def process(spark, train_data, test_data):
    #train_data - путь к файлу с данными для обучения модели
    #test_data - путь к файлу с данными для оценки качества модели
    
    #Загрузим датасеты
    trainSet = pqt_into_spark(spark, train_data)
    testSet = pqt_into_spark(spark, test_data)
    
    # составим feature vectors (не используя ad_id)
    # Переименуем 'ctr' в 'label' - нужно для кросс-валидации
    trainSet = trainSet.withColumnRenamed("ctr", "label")
    testSet = testSet.withColumnRenamed("ctr", "label")
    features = VectorAssembler(inputCols = trainSet.columns[1:-1], outputCol = 'features')
    
    # Создадим индексированный вектор для категориальных фич
    features_indexing = VectorIndexer(inputCol = 'features', outputCol = 'indexedFeatures', maxCategories = 2)
    
    # Создадим этап пайплайна для тренировки моделей
    lr = LinearRegression(featuresCol = 'features', labelCol = 'label')
    (dt, rf, gbt) = [x(featuresCol = 'indexedFeatures', 
                       labelCol = 'label') 
                     for x in [DecisionTreeRegressor, RandomForestRegressor, GBTRegressor]]
    
    # Соберем пайплайны для моделей
    pipelines = {
    "pipelineLR" : Pipeline(stages = [features, lr]),
    "pipelineDT" : Pipeline(stages = [features, features_indexing, dt]),
    "pipelineRF" : Pipeline(stages = [features, features_indexing, rf]),
    "pipelineGBT" : Pipeline(stages = [features, features_indexing, gbt])
    }
    
    # Соберем paramGrids для кросс-валидации
    paramGrids = {
        "paramGridLR" : ParamGridBuilder() \
            .addGrid(lr.maxIter, [10, 20, 40, 80, 150, 300]) \
            .addGrid(lr.regParam, [0.1, 0.2, 0.4, 0.6, 0.8, 0.9]) \
            .addGrid(lr.elasticNetParam, [0.5, 0.6, 0.7, 0.8, 0.9])\
            .build(),
        "paramGridDT" : ParamGridBuilder() \
            .addGrid(dt.maxBins, [24, 28, 32, 36, 40]) \
            .addGrid(dt.maxDepth, [3, 4, 5, 6, 7]) \
            .build(),
        "paramGridRF" : ParamGridBuilder() \
            .addGrid(rf.numTrees, [10, 15, 20, 25, 30]) \
            .addGrid(rf.maxBins, [24, 28, 32, 36, 40]) \
            .addGrid(rf.maxDepth, [3, 4, 5, 6, 7]) \
            .build(),
        "paramGridGBT" : ParamGridBuilder() \
            .addGrid(gbt.maxDepth, [3, 4, 5, 6, 7]) \
            .addGrid(gbt.maxBins, [24, 28, 32, 36, 40]) \
            .addGrid(gbt.stepSize, [0.05, 0.1, 0.2]) \
            .build()
    }
    
    # подготовим разные кросс-валидированные модели -- ОЧЕНЬ долгий этап
    cv = {modeltype : CrossValidator(estimator=pipelines["pipeline" + modeltype],
                          estimatorParamMaps=paramGrids["paramGrid" + modeltype],
                          evaluator=RegressionEvaluator(),
                          numFolds=3, parallelism = 5).fit(trainSet)
          for modeltype in ["LR", "DT", "RF", "GBT"]}
    
    # подготовим предсказания с использованием этих кросс-валидированных моделей, посчитаем RMSE и выберем лучшую
    cv_predictions = {modeltype : cv[modeltype].transform(testSet) for modeltype in cv}
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluation = {modeltype : evaluator.evaluate(cv_predictions[modeltype]) for modeltype in cv_predictions}

    # Выберем модель с наименьшей RMSE и сохраним в указанную директорию
    bestModel = cv[min(evaluation.items())[0]]
    bestModel.save(MODEL_PATH)
    
def pqt_into_spark(spark, input_file):
    # Эта функция прицельно загружает паркет-файл в спарк
    sparkdata = spark.read.parquet(input_file)
    return sparkdata

def main(argv):
    train_data = argv[0]
    print("Input path to train data: " + train_data)
    test_data = argv[1]
    print("Input path to test data: " + test_data)
    spark = _spark_session()
    process(spark, train_data, test_data)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Train and test data are require.")
    else:
        main(arg)
