package artunc.sparkml.api.model.metadata;

import lombok.Data;

@Data
public class PerformanceMetrics {
    private double f1;
    private double precision;
    private double recall;
    private double accuracy;
    private double auroc;
    private double aupr;
    private String confusionMatrixStr;
}
