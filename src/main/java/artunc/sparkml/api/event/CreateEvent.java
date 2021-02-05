package artunc.sparkml.api.event;

import lombok.Data;

@Data
public class CreateEvent {
    private String modelName;
    private String targetColumn;
    private String columns;
    private String modelType;
    private String dataFileName;
}
