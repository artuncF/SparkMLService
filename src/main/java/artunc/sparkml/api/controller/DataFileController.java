package artunc.sparkml.api.controller;

import artunc.sparkml.api.service.FileService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@Slf4j
public class DataFileController {

    private FileService dataFileService;
    @Autowired
    public void setFileService(FileService service){
        this.dataFileService =service;
    }

    @GetMapping("/files/list")
    public List<String> getAllFiles(){
        return this.dataFileService.listFiles();
    }

    @PostMapping("/files/upload")
    public String uploadFile(@RequestParam("file") MultipartFile file){
        return this.dataFileService.addFile(file)?"SUCCESS":"FAIL";
    }
}
