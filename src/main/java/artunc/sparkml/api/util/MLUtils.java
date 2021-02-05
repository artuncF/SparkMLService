package artunc.sparkml.api.util;

import artunc.sparkml.api.model.metadata.PerformanceMetrics;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MLUtils {

    public static final String F_1 = "f1";
    public static final String ACCURACY = "accuracy";
    public static final String WEIGHTED_PRECISION = "weightedPrecision";
    public static final String WEIGHTED_RECALL = "weightedRecall";

    private MLUtils() {
    }

    public static double[][] fillConfusionMatrix(Dataset<Row> predsWithLabels, double[][] confusionMatrix) {
        Iterator<Row> rowIterator = predsWithLabels.toLocalIterator();
        while (rowIterator.hasNext()) {
            Row currentRow = rowIterator.next();
            int pred = (int) Double.parseDouble(currentRow.get(0).toString());
            int actual = (int) Double.parseDouble(currentRow.get(1).toString());
            confusionMatrix[pred][actual]++;
        }
        return confusionMatrix;
    }

    public static double calcAccuracyForBinary(double[][] confMatrix) {
        double tn = confMatrix[0][0];
        double fp = confMatrix[0][1];
        double fn = confMatrix[1][0];
        double tp = confMatrix[1][1];

        return (tp + tn) / (tp + tn + fp + fn);
    }

    public static double calcPrecisionForBinary(double[][] confMatrix) {
        double tp = confMatrix[1][1];
        double fp = confMatrix[0][1];

        return tp / (tp + fp);
    }

    public static double calcRecallForBinary(double[][] confMatrix) {
        double tp = confMatrix[1][1];
        double fn = confMatrix[1][0];
        return tp / (tp + fn);
    }

    public static double calcFMeasureForBinary(double[][] confMatrix) {
        double cPrecision = calcPrecisionForBinary(confMatrix);
        double cRecall = calcRecallForBinary(confMatrix);
        return 2 * cPrecision * cRecall / (cPrecision + cRecall);
    }

    public static String toStringConfusionMatrix(double[][] confMatrix) {
        return Stream.of(confMatrix)
                    .map(Arrays::toString)
                    .collect(Collectors.joining(System.lineSeparator()));
    }

    public static void setMultiClassMetrics(MulticlassClassificationEvaluator evaluator, PerformanceMetrics modelPerf, Dataset<Row> predictions){
        modelPerf.setF1(evaluator.setMetricName(F_1).evaluate(predictions));
        modelPerf.setAccuracy(evaluator.setMetricName(ACCURACY).evaluate(predictions));
        modelPerf.setPrecision(evaluator.setMetricName(WEIGHTED_PRECISION).evaluate(predictions));
        modelPerf.setRecall(evaluator.setMetricName(WEIGHTED_RECALL).evaluate(predictions));
    }

    public static void setBinaryClassMetrics(BinaryClassificationEvaluator evaluator, PerformanceMetrics modelPerf, Dataset<Row> predictions){
        modelPerf.setF1(evaluator.setMetricName(F_1).evaluate(predictions));
        modelPerf.setAccuracy(evaluator.setMetricName(ACCURACY).evaluate(predictions));
        modelPerf.setPrecision(evaluator.setMetricName(WEIGHTED_PRECISION).evaluate(predictions));
        modelPerf.setRecall(evaluator.setMetricName(WEIGHTED_RECALL).evaluate(predictions));
    }
}
