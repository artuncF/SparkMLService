package artunc.sparkml.api.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class FileService {

    @Value("${spring.servlet.multipart.location}")
    private String dataFileDir;

    private static final String DELIMITER = ",";
    private static final String CSV_REGEX = "([a-zA-Z0-9\\s_\\\\.\\-:])+(.csv)$";

    public List<String> listFiles(){
        return Stream.of(Objects.requireNonNull(new File(dataFileDir).listFiles()))
                .map(File::getName)
                .filter(filename->filename.matches(CSV_REGEX))
                .collect(Collectors.toList());
    }

    public boolean addFile(MultipartFile file){
        String originalFilename = file.getOriginalFilename();
        assert originalFilename != null;
        String path = Paths.get(dataFileDir, originalFilename).toString();
        File file1 = new File(path);

        if (!isNameValid(file1.getName()) && !isFileValid(file1.getPath())){
            log.debug("File format verification failed.");
            return false;
        }
        try {
            file.transferTo(file1);
            return true;
        } catch (IOException e) {
            log.debug(e.getLocalizedMessage());
            return false;
        }
    }

    private boolean isNameValid (String fileName){
        return fileName.matches(CSV_REGEX);
    }

    private boolean isFileValid (String filePath){
        try (Stream<String> fileStream = Files.lines(Paths.get(filePath))) {
            return fileStream
                    .map(line->line.split(DELIMITER).length!=0)
                    .reduce(Boolean::logicalAnd)
                    .orElse(false);
        } catch (IOException e) {
            log.debug(e.getLocalizedMessage());
            return false;
        }
    }

}
