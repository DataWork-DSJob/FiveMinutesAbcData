package com.github.testing.jsonfilesender;

import com.beust.jcommander.IStringConverter;

import java.io.File;
import java.net.URL;

public class FileConvert implements IStringConverter<File> {

    @Override
    public File convert(String path) {
        File file = null;
        if (null != path && !path.isEmpty()) {
            file = new File(path);
            if (file.exists()) {
                return file;
            } else {
                URL url = this.getClass().getClassLoader().getResource(path);
                if (null != url) {
                    url.getFile();
                    file = new File(url.getFile());
                } else {
                    file = null;
                }
            }
            if (null == file || !file.exists()) {
                throw new IllegalArgumentException(path + " not exists 路径错误");
            }
        }
        return file;
    }
    
}
