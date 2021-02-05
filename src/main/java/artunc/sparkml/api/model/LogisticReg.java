package artunc.sparkml.api.model;


import artunc.sparkml.api.model.metadata.ModelInfo;
import org.apache.spark.ml.classification.LogisticRegression;

import static artunc.sparkml.api.util.Constants.FEATURES;
import static artunc.sparkml.api.util.Constants.INDEXEDLABEL;

public class LogisticReg extends BatchClassifier {
    public LogisticReg (ModelInfo modelMetadata) {
        super(modelMetadata);
        this.buildModel();
    }

    @Override
    protected void buildModel () {
        LogisticRegression logisticRegression = new LogisticRegression()
                .setLabelCol(INDEXEDLABEL)
                .setFeaturesCol(FEATURES);

        setupModel(logisticRegression);
    }
}
