package artunc.sparkml.api.model.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.spark.sql.types.StructType;

import java.util.List;

@Data
@EqualsAndHashCode
public class ModelInfo {
    private String modelId;
    private String modelName;
    private String modelType;
    private String dataPath;
    private String targetColumnName;
    private String[] classLabels;

    private int numOfClasses;
    private PerformanceMetrics modelPerf;

    private StructType schema;
    private List<String> columns;
    private double[][] confMatrix;

    public ModelInfo(){
        initConfMatrix();
    }

    public void initConfMatrix(){
        this.confMatrix = new double[this.numOfClasses][this.numOfClasses];
    }
}


