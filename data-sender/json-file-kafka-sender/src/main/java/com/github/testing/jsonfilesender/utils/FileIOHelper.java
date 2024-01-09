package com.github.testing.jsonfilesender.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FileIOHelper {



    /**
     * 推荐这个方法: ByByteOutStream, 这里就是一 buff 字节的方式, 最原始的转换;
     * @return
     * @throws IOException
     */
    public String readStringCtx(File file) throws IOException {
        final int buffSize = 1024;
        InputStream fio = new FileInputStream(file);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buff = new byte[buffSize];
        int length = -1;
        while ((length = fio.read(buff)) != -1) {
            bos.write(buff, 0, length);
        }
        bos.close();
        fio.close();
        String strContext = new String(bos.toByteArray(), "utf-8");
        return strContext;
    }

    public List<String> readAllLines(String filePath) throws IOException {
        return Files.readAllLines(Paths.get(filePath), Charset.forName("utf-8"));
    }


    public static JSONObject readPointFileAsBean(File file)  {
        JSONObject pointTemplateArrays = null;
        InputStream fio = null;
        try {
            fio = new FileInputStream(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(fio));
            StringBuffer buffer = new StringBuffer();
            String str = null;
            while ((str = br.readLine()) != null) {
                buffer.append(str.trim()).append("\n");
            }
            String strContext = buffer.toString();
            pointTemplateArrays = JSON.parseObject(strContext);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getCause());
        } finally {
            if (null != fio) {
                try {
                    fio.close();
                } catch (IOException e) {
                    fio = null;
                }
            }
        }
        return pointTemplateArrays;
    }



}
