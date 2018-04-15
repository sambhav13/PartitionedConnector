package com.app.partitioner;

import com.app.writer.HdfsWriter;

/**
 * Created by samgupta0 on 4/15/2018.
 */
public class PartitionHandler {


    private HdfsWriter writer;

    public PartitionHandler(String fs){

        this.writer =  new HdfsWriter(fs);
    }

    public void partionWrite(String record,int partition){
            this.writer.write(record,partition);

    }
}
