package artunc.sparkml.api.model;

import artunc.sparkml.api.model.metadata.ModelInfo;
import artunc.sparkml.api.util.Constants;
import artunc.sparkml.api.util.MLUtils;
import artunc.sparkml.api.util.SparkUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Optional;

@Data
@Slf4j
public abstract class BatchClassifier {


    public static final String AREA_UNDER_ROC = "areaUnderROC";
    public static final String AREA_UNDER_PR = "areaUnderPR";
    protected Pipeline clfPipeline;
    protected PipelineModel model;
    private ModelInfo metadata;

    public BatchClassifier (ModelInfo modelMetadata) {
        this.metadata = modelMetadata;
        this.metadata.initConfMatrix();
    }

    protected abstract void buildModel ();

    protected  <T> void setupModel (T model) {
        assert !(model instanceof PipelineStage);

        String targetColumnName = this.getMetadata().getTargetColumnName();
        List<String> columns = this.getMetadata().getColumns();

        VectorAssembler featureColumnAssembler = SparkUtils.getColumnAssembler(targetColumnName, columns);
        StringIndexer labelIndexer = SparkUtils.getLabelIndexer(targetColumnName);

        PipelineStage[] stages = {featureColumnAssembler,
                labelIndexer,
                (PipelineStage)model};

        this.clfPipeline = this.buildPipeline(stages);
        this.model = null;
    }

    public boolean train (Dataset<Row> trainingData) {
        try {
            String[] classLabels = SparkUtils.getClassLabels(trainingData, this.metadata.getTargetColumnName());
            this.metadata.setClassLabels(classLabels);
            this.model = this.clfPipeline.fit(trainingData);
            return true;
        } catch (Exception e) {
            log.debug(e.getCause().toString());
            return false;
        }
    }

    protected Dataset<Row> test (Dataset<Row> testData) {
        return this.model.transform(testData)
                .select(Constants.PREDICTION, Constants.INDEXEDLABEL);
    }


    protected Optional<String> predict (Dataset<Row> predictInstance) {
        if (this.model == null)
            return Optional.empty();

        try {
            Dataset<Row> prediction = this.model.transform(predictInstance);
            String predictedLabel = SparkUtils.getPredictLabel(prediction, this.metadata.getClassLabels());
            return Optional.of(predictedLabel);
        } catch (Exception e) {
            log.debug(e.getCause().toString());
            return Optional.empty();
        }
    }

    protected Optional<String> evaluate (Dataset<Row> predictions) {
        try {
            if (this.getMetadata().getClassLabels().length > 2) {
                MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                        .setLabelCol(Constants.INDEXEDLABEL)
                        .setPredictionCol(Constants.PREDICTION);
                MLUtils.setMultiClassMetrics(evaluator, this.getMetadata().getModelPerf(), predictions);
                MLUtils.fillConfusionMatrix(predictions, this.metadata.getConfMatrix());
                String confMatrixStr = MLUtils.toStringConfusionMatrix(this.metadata.getConfMatrix());
                this.metadata.getModelPerf().setConfusionMatrixStr(confMatrixStr);
            } else {
                BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                        .setLabelCol(Constants.INDEXEDLABEL)
                        .setRawPredictionCol(Constants.PREDICTION);
                this.metadata.getModelPerf().setAuroc(evaluator.setMetricName(AREA_UNDER_ROC).evaluate(predictions));
                this.metadata.getModelPerf().setAupr(evaluator.setMetricName(AREA_UNDER_PR).evaluate(predictions));
                MLUtils.fillConfusionMatrix(predictions, this.metadata.getConfMatrix());
                MLUtils.setBinaryClassMetrics(evaluator, this.getMetadata().getModelPerf(), predictions);
                String confMatrixStr = MLUtils.toStringConfusionMatrix(this.metadata.getConfMatrix());
                this.metadata.getModelPerf().setConfusionMatrixStr(confMatrixStr);
            }
            return Optional.of(this.metadata.getModelPerf().toString());
        } catch (Exception e) {
            log.debug(e.getCause().toString());
            return Optional.empty();
        }
    }

    protected Pipeline buildPipeline (PipelineStage[] stages) {
        return new Pipeline().setStages(stages);
    }


}
