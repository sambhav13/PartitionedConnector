package com.app.writer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by samgupta0 on 4/15/2018.
 */
public class HdfsWriter {

    private Configuration conf;
    private  FileSystem fs;
    private String partition;

    public HdfsWriter(String fs){

       conf = new Configuration();
       conf.set("fs.default.name", fs);
       fs = null;

     }

     public void write(String record,int partition){

         FileSystem fs = null;

         try {
             fs = FileSystem.get(conf);

             Path dir = new Path("/kafka/data/avro");
             if(!fs.exists(dir))
                 fs.mkdirs(dir);

            // new FileOutputStream(new File("").)
             OutputStream os = fs.create(new Path("/kafka/data/avro/"+"Target-000-"+partition));

             byte[]  finalByte = record.getBytes();
             os.write(finalByte);

             os.close();
         }catch (IOException io){
             io.printStackTrace();
         }finally {
             try {
                 fs.close();
             } catch (IOException e) {
                 e.printStackTrace();
             }
         }
     }

}
