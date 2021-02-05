package artunc.sparkml.api.factory;

import artunc.sparkml.api.model.BatchClassifier;
import artunc.sparkml.api.model.LogisticReg;
import artunc.sparkml.api.model.RandomForestClf;
import artunc.sparkml.api.model.metadata.ModelInfo;
import lombok.NoArgsConstructor;

import java.util.Optional;

@NoArgsConstructor
public class ModelFactory {
     public Optional<BatchClassifier> buildModel(ModelInfo metadata){
         String modelType = metadata.getModelType();
         if(modelType.equalsIgnoreCase("logreg")){
             return Optional.of(new LogisticReg(metadata));
         }else if (modelType.equalsIgnoreCase("randomforest")){
             return Optional.of(new RandomForestClf(metadata));
         }else {
             return Optional.empty();
         }
     }
}
