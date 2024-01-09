package flink.debug.utils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DebugCommMethod implements Serializable {


    public Path getOrCreateDirFromUserDir(String dirName) {
        if (null == dirName || dirName.isEmpty()) {
            throw new IllegalArgumentException("dirName cannot be null");
        }
        Path dirPath = Paths.get(System.getProperty("user.dir"), dirName);
        if (!Files.exists(dirPath)) {
            try {
                dirPath = Files.createDirectory(dirPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return dirPath.toAbsolutePath();
    }


}
