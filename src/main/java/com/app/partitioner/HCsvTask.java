package com.app.partitioner;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by samgupta0 on 4/15/2018.
 */
public class HCsvTask extends SinkTask {

    private static Logger log = LogManager.getLogger(HCsvTask.class);
    public String version() {
        return null;
    }
    //private AvroWriter writer;
    static int counter = 0;
    private String topic;
    private String fs;


    private PartitionHandler partitionHandler;

    public void start(Map<String, String> map) {

        //writer = new AvroWriter();
        log.info("initialized the HDFS Sink ");
        this.context.timeout(2000);
        this.topic = map.get("topic");
        this.fs = map.get("fs.default.name");
        this.partitionHandler = new PartitionHandler(this.fs);
    }

    public void put(Collection<SinkRecord> collection) {

        try {
           /* Collection<String> recordsAsString = collection.stream().map(r -> String.valueOf(r.value()))
                    .collect(Collectors.toList());*/
            System.out.println("collection size --> "+collection.size());
            //collection.stream().map(r -> String.valueOf(r.value()))
            if(collection.size()>0) {
                System.out.println("before iterator");
                Iterator<SinkRecord> it = collection.iterator();
                System.out.println("after iterator");
                process(it);
            }
        }
        catch (Exception e) {
            log.error("Error while processing records");
            log.error(e.toString());
        }
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        log.trace("Flushing the queue");
    }

    public void stop() {

    }

    public void process(Iterator<SinkRecord> it) {

        log.info("processing the record");

        System.out.println("started processing");

        if(it!=null) {

            Object p = null;
            Object v = null;
            try {
                System.out.println("has or not" + it.hasNext());
                SinkRecord rec = it.next();
                p = rec.value();
                v = rec.valueSchema();

                int partition = rec.kafkaPartition();
                long offset = rec.kafkaOffset();

                this.partitionHandler.partionWrite(p.toString(),partition);
                /*ConnectSchema valSchema = (ConnectSchema)v;
                List<Field> fields = valSchema.fields();
                for(Field f:fields){
                    System.out.println("field name is-->"+f.name());
                    System.out.println("field schema is-->"+f.schema());
                }*/
            }catch (Exception ex){
                ex.printStackTrace();
            }

            try {
                counter++;
                //writer.write("avro_" + counter + ".avro", p.toString());
                System.out.println("after write");
            }  catch (Exception ex) {
                ex.printStackTrace();
            }


        }

    }



    class RecordSchema{
        private Object value;
        private Schema typeInfo;

        public RecordSchema( Schema typeInfo,Object value) {
            this.value = value;
            this.typeInfo = typeInfo;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Schema getTypeInfo() {
            return typeInfo;
        }

        public void setTypeInfo(Schema typeInfo) {
            this.typeInfo = typeInfo;
        }
    }
}
