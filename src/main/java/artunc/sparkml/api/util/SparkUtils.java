package artunc.sparkml.api.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Optional;

import static artunc.sparkml.api.util.Constants.*;

@Slf4j
public class SparkUtils {
    private SparkUtils(){}

    public static Optional<Dataset<Row>> readData(SparkSession ss, StructType schema, String dataPath){
        try {
            return Optional.of(ss.read()
                    .schema(schema)
                    .load(dataPath)
                    .toDF());
        } catch (Exception e) {
            log.debug("Cannot read data.");
            return Optional.empty();
        }
    }

    public static StructType getDataConfig(List<String> columns, String targetColumnName){
        StructField[] fieldsConf = columns
                                    .stream()
                                    .map(el -> {
                                        if (!el.contains(targetColumnName)) {
                                            return new StructField(el, DataTypes.DoubleType, false, Metadata.empty());
                                        }
                                        return new StructField(el, DataTypes.StringType, false, Metadata.empty());
                                    }).toArray(StructField[]::new);
        return new StructType(fieldsConf);
    }

    public static String[] getClassLabels(Dataset<Row> data, String targetColumnName) {
        return data.select(targetColumnName)
                .distinct()
                .collectAsList()
                .parallelStream()
                .map(row -> row.getString(0))
                .toArray(String[]::new);
    }

    public static String getPredictLabel(Dataset<Row> prediction, String[] classLabels){
        IndexToString labelConverter = getLabelFromNumeric(classLabels);
        return labelConverter.
                transform(prediction)
                .select(PREDICTEDLABEL)
                .head()
                .toString();
    }

    public static IndexToString getLabelFromNumeric(String[] classLabels){
        return new IndexToString()
                .setInputCol(PREDICTION)
                .setOutputCol(PREDICTEDLABEL)
                .setLabels(classLabels);
    }

    public static StringIndexer getLabelIndexer(String targetColumnName){
        return new StringIndexer()
                .setInputCol(targetColumnName)
                .setOutputCol(INDEXEDLABEL);
    }

    public static VectorAssembler getColumnAssembler(String targetColumnName, List<String> columns) {
        return new VectorAssembler()
                .setInputCols(SparkUtils.getOnlyFeatureColumnNames(columns, targetColumnName))
                .setOutputCol(FEATURES);
    }

    public static String[] getOnlyFeatureColumnNames(List<String> columnNames, String targetColumnName) {
        return columnNames.stream()
                .filter(el -> !el.toLowerCase().contains(targetColumnName))
                .toArray(String[]::new);
    }

}
