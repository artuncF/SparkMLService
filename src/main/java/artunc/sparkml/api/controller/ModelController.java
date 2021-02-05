package artunc.sparkml.api.controller;

import artunc.sparkml.api.event.CreateEvent;
import artunc.sparkml.api.service.ModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;


@RestController
@Slf4j
@CrossOrigin(origins = "http://localhost:3000")
public class ModelController {

    private final ModelService classificationService;
    @Autowired
    public ModelController(ModelService service){
        this.classificationService = service;
    }

    @GetMapping("/models/list")
    public List<String> getAllModelInfo () {
        return new ArrayList<>(this.classificationService.getModels()
                .keySet());
    }

    @PostMapping("/models/create")
    public ResponseEntity<String> create (@RequestBody CreateEvent event) {
        log.info(event.toString());
        return new ResponseEntity<>(this.classificationService.create(event) ? "Model created" : "Model cannot be created", HttpStatus.OK);
    }

    @DeleteMapping("/models/delete/{modelId}")
    public String delete (@PathVariable String modelId) {
        return this.classificationService.delete(modelId) ? "Model deleted" : "Model cannot be deleted because it does not exist";
    }

    @PostMapping("/models/train/{modelId}")
    public String train (@PathVariable String modelId) {

        return this.classificationService.train(modelId) ? "Model trained successfully and ready to predict." :
                "Model cannot be trained successfully";
    }
}
