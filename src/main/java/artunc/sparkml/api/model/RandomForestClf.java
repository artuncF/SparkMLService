package artunc.sparkml.api.model;

import artunc.sparkml.api.model.metadata.ModelInfo;
import org.apache.spark.ml.classification.RandomForestClassifier;

import static artunc.sparkml.api.util.Constants.FEATURES;
import static artunc.sparkml.api.util.Constants.INDEXEDLABEL;

public class RandomForestClf extends BatchClassifier {

    public RandomForestClf(ModelInfo modelMetadata) {
        super(modelMetadata);
        this.buildModel();
    }

    @Override
    protected void buildModel() {
         RandomForestClassifier randomForestHypothesis = new RandomForestClassifier()
                .setLabelCol(INDEXEDLABEL)
                .setFeaturesCol(FEATURES);

        setupModel(randomForestHypothesis);
    }

}
