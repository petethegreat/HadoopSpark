
#!/usr/local/spark/spark-2.2.0-bin-hadoop2.7/bin/pyspark


from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler, Normalizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.ml import Pipeline, PipelineModel
import sys

############################################################

def PredictTest(
        spark,
        titanicTestFile,
        modelFileName,
        predictionFile='hdfs://localhost:54310/user/hduser/output/titanic_lr_prediction',
        modelPredColName='lr_survivePred'
        ):
    # load the test data
    test = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option('inferSchema','true') \
        .load(titanicTestFile) 

    # load the saved model
    loadedModel = PipelineModel.load(modelFileName)
    # transform (predict on) the test data
    predicted = loadedModel.transform(test)
    # write predictions to file
    predicted.select(['PassengerId',predicted[modelPredColName].cast('int').alias('Survived')]) \
        .coalesce(1) \
        .write.csv(path=predictionFile,header=True,mode='overwrite')
    print('wrote test predictions to {fd}'.format(fd=predictionFile))
        # cast survived to int
############################################################

def fitLRmodel(spark,titanicTrainFile,modelDest):
    # Try a logistic regression first
    # Pipeline stuff
    # We want a pipeline that does the following things:
    #   - extracts title from name
    #   - encodes sex and embarked as one hot (stringIndexer)
    #   - imputes missing age based on pclass and sex
    #   - combines desired features into a feature vector
    #   - fits the lr to the features based on Survived

    # read the data from our csv
    data = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option('inferSchema','true') \
        .load(titanicTrainFile)

    # parition into training and validation sets
    train,val = data.randomSplit([0.8,0.2],seed=1991)

    train.limit(10).show()
    train.printSchema()
    train.filter(train.Embarked.isNull()).select('PassengerId','Name','Embarked').limit(10).show()
    train.agg({'Embarked':'count'}).collect()

    nullFares = train.filter(train.Fare.isNull()).count()
    totalRows = train.count()
    print('Fare: {nf} of {nnf} rows missing'.format(nf=nullFares,nnf=totalRows))

    # set up transformation pipeline stages
    # index "Sex" column and encode as one hot
    sexIndexer = StringIndexer(
        inputCol='Sex',
        outputCol='sex_word')
    sexEncoder = OneHotEncoder(
        inputCol=sexIndexer.getOutputCol(),
        outputCol='sex_cat',
        dropLast=False)
    # encode "Pclass" as one hot (rather than int)
    classEncoder = OneHotEncoder(
        inputCol='Pclass',
        outputCol='pclass_cat',
        dropLast=False)
    #index "Embarked" and encode as one hot. Keep null values
    embarkIndexer = StringIndexer(
        inputCol='Embarked',
        outputCol='embarked_word',
        handleInvalid="keep")
    embarkEncoder = OneHotEncoder(
        inputCol=embarkIndexer.getOutputCol(),
        outputCol='embarked_cat',
        dropLast=False)
    # impute missing age values
    ageFareImputer = Imputer(
        inputCols=['Age','Fare'],
        outputCols=['age_i','fare_i'])

    # combine age, Parch, and SibSp into a single vector
    ASPFvecAssembler = VectorAssembler(
        inputCols=['age_i','Parch','SibSp','fare_i'],
        outputCol='ASPF_raw')
    # shift and scale ASP vector
    ASPFscaler = StandardScaler(inputCol=ASPFvecAssembler.getOutputCol(),
        outputCol='ASPF_scaled',
        withMean=True,
        withStd=True)

    # Create a feature vector
    # first generate a list of column names
    featureCols = [x.getOutputCol() for x in [
        sexEncoder,
        classEncoder,
        embarkEncoder,
        ASPFscaler,
        ]]

    # assemble into a vector
    featureVecAssembler = VectorAssembler(
        inputCols=featureCols,
        outputCol='features')

    # logistic regression
    # regularisation parameters will be tuned via cross validation
    lr = LogisticRegression(
        maxIter=10,
        family='binomial',
        # regParam=0.0,
        labelCol='Survived',
        featuresCol='features',
        probabilityCol='lr_surviveProb',
        predictionCol='lr_survivePred',
        rawPredictionCol='lr_rawPrediction')

    # rawPrediction is w*x, which is the value of the logit function for this observation
    # probability is the conditional probability (the modeled probability given the data for this observation to belong to class 0)
    # prediction is the class (label) that has the highest probability
    

    # Assemble all the components
    pipestages = [sexIndexer,
                  sexEncoder,
                  classEncoder,
                  embarkIndexer,
                  embarkEncoder,
                  ageFareImputer,
                  ASPFvecAssembler,
                  ASPFscaler,
                  featureVecAssembler,
                  lr
                  ]
    # construct a pipeline
    pipeline = Pipeline(stages=pipestages)

    # specify parameter grid for cv tuning
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam,[0.0,0.01,0.1]) \
        .addGrid(lr.elasticNetParam, [0.0, 1.0]) \
        .build()
    # initialise an evaluator. This uses ROC AUC as a metric by default.
    BCeval = BinaryClassificationEvaluator(labelCol='Survived',rawPredictionCol=lr.getRawPredictionCol())

    # initialise the cross validator object
    # will use 3 fold cross validation
    cv = CrossValidator(estimator=pipeline,estimatorParamMaps=paramGrid,evaluator=BCeval,numFolds=3,seed=1992)

    # Carry out the cross validation and obtain the model with the best performance
    cvModel = cv.fit(train)
    # print some performance metrics (these are averaged over the three folds)
    print('CrossValidation average metrics:')
    print(cvModel.avgMetrics)

    trained = cvModel.transform(train)
    trainedEval = BCeval.evaluate(trained)
    print('best model evaluation on training set = {v:0.4g}'.format(v=trainedEval))

    validation = cvModel.transform(val)
    # evaluate model on validation set
    valEval = BCeval.evaluate(validation)
    print('best model evaluation on validation set = {v:0.4g}'.format(v=valEval))
    cvModel.bestModel.write().overwrite().save(modelDest)
    # model.write.overwrite().save("/tmp/spark-logistic-regression-model")
############################################################

def main():
    ''' train a model, save to disk, load, and predict on test set'''

    # Set up spark configuration
    conf = SparkConf()
    conf.setMaster("spark://ravage:7077")
    conf.setAppName("pyspark titanic csv/ML pipeline test")
    conf.set("spark.executor.memory", "512m")
    conf.set("sparkHome","/usr/local/spark/spark-2.2.0-bin-hadoop2.7")

    # spark used to focus on rdds, and sparkcontext was the old entry point for working with them
    # For newer versions of spark (2.0?) datasets (generalisatio of dataframe) is default, and sparksession is used for these
    # sparksession has a sparkcontext running in its background.

    # initialise the SparkSession
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    # train and test filenames (hdfs urls)
    titanicTrainFile = 'hdfs://localhost:54310/user/hduser/input/titanic_train.csv'
    titanicTestFile = 'hdfs://localhost:54310/user/hduser/input/titanic_test.csv'
    LRmodelFilename = 'hdfs://localhost:54310/user/hduser/models/titanic_lr'
    predictionFile = 'hdfs://localhost:54310/user/hduser/output/titanic_lr_prediction'

    # fit (and save) our model
    fitLRmodel(
        spark,
        titanicTrainFile,
        LRmodelFilename)

    # load our model, and predict on the test set
    PredictTest(
        spark,
        titanicTestFile,
        LRmodelFilename,
        predictionFile=predictionFile,
        modelPredColName='lr_survivePred'
        )

    spark.stop()
############################################################


if __name__ == '__main__':
    main()
    


    # pipeModel = pipeline.fit(train)
    # pipeModel is now an estimator (transformer), and can be used in other pipelines (?)

    # does our trained model have a summary?
    # print('final pipemodel stage does {notval}have a summary'.format(notval='' if pipeModel.stages[-1].hasSummary else 'NOT '))

    # lrmodel = pipeModel.stages[-1]
    # if lrmodel.hasSummary:
    #     ts=lrmodel.summary
    #     ts.roc.show()
    #     ts.roc.coalesce(1).write.csv('hdfs://localhost:54310/user/hduser/output/lr_roc.csv')
    #     print('training Objective History:')
    #     for obj in ts.objectiveHistory:
    #         print(obj)


    # evaluate model
    # thedata = pipeModel.transform(val)

    # thedata.printSchema()

    # thedata.select(['PassengerId','Survived',lr.getRawPredictionCol(),lr.getProbabilityCol(),lr.getPredictionCol()]).limit(20).show()



    # Todo:
    # use cross validation (in training) to get model hyperparameters
    # evaluate model performance (both on training and validation set)
    # make an ensemble classifier (say, a logistic regression based on the output probabilities of constituent classifiers)
    # train ensemble on validation data
    #  

    # print('** Null Features **\n')
    # check = ['Embarked','age_i','PassengerId','Survived','Cabin']
    # for col in thedata.columns:
    #     count=thedata.filter(thedata[col].isNull()).count()
    #     print('{col}: {count}'.format(col=col,count=count))

    # thedata.groupBy('embarked_word').count().show()
    # thedata.groupBy('Sex').count().show()
    # thedata.groupBy('Pclass').count().show()
    # thedata.filter(thedata['age_i'].isNull()).count()
    # thedata.filter(thedata.embarked_word.isNull()).select(['Embarked','Name','embarked_cat','features']).limit(10).show()
    # thedata.select('Survived','predictedSurvived','probSurvived').limit(10).show()



