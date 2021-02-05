package artunc.sparkml.api.service;

import artunc.sparkml.api.event.CreateEvent;
import artunc.sparkml.api.factory.ModelFactory;
import artunc.sparkml.api.model.BatchClassifier;
import artunc.sparkml.api.model.metadata.ModelInfo;
import artunc.sparkml.api.util.SparkUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
@Getter
public class ModelService {
    private final HashMap<String, Optional<BatchClassifier>> models = new HashMap<>();
    private final SparkSession sess;
    private final ModelFactory factory = new ModelFactory();

    @Autowired
    public ModelService (SparkSession session){
        this.sess = session;
    }

    public boolean create(CreateEvent event){

        ModelInfo newModelMetadata = initModelMetadata(event);
        boolean isSuccess = true;
        try {
            BatchClassifier newModel = factory.buildModel(newModelMetadata).orElseThrow(() -> new Exception("Model type cannot be found"));
            models.put(newModelMetadata.getModelId(),Optional.of(newModel));
        } catch (Exception e) {
            log.debug(e.getLocalizedMessage());
            isSuccess = false;
        }
        return isSuccess;
    }

    private ModelInfo initModelMetadata (CreateEvent event) {
        ModelInfo newModelMetadata = new ModelInfo();
        List<String> columns = Arrays.asList(event.getColumns().split(","));
        String dataFileName = event.getDataFileName();
        String modelName = event.getModelName();
        String modelType = event.getModelType();
        String targetColumn = event.getTargetColumn();

        newModelMetadata.setModelId(modelType + "_" + modelName);
        newModelMetadata.setDataPath(dataFileName);
        newModelMetadata.setModelName(modelName);
        newModelMetadata.setModelType(modelType);
        newModelMetadata.setColumns(columns);
        newModelMetadata.setTargetColumnName(targetColumn);
        newModelMetadata.setSchema(SparkUtils.getDataConfig(columns, targetColumn));
        return newModelMetadata;
    }

    public boolean delete(String modelId){
        boolean isSuccess = true;
        try {
            models.get(modelId).orElseThrow(()->new Exception("Model cannot be found!"));
            models.remove(modelId);
        } catch (Exception e) {
            log.debug(e.getLocalizedMessage());
            isSuccess = false;
        }
        return isSuccess;
    }

    public boolean train(String modelId) {
        try {
            BatchClassifier model = models.get(modelId).orElseThrow(()->new Exception("Model cannot be found!"));

            Dataset<Row> trainData = SparkUtils.readData(this.sess, model.getMetadata().getSchema(), model.getMetadata().getDataPath())
                    .orElseThrow(()->new Exception("Training data cannot be loaded."));
            return model.train(trainData);
        } catch (Exception e) {
            log.debug(e.getLocalizedMessage());
            return false;
        }
    }

    public void predict(Map<String,Integer> predictEvent){

    }

}
