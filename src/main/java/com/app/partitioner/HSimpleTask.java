package com.app.partitioner;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by samgupta0 on 4/15/2018.
 */
/*public class HSimpleTask extends SinkTask {

    private static Logger log = LogManager.getLogger(HSimpleTask.class);
    public String version() {
        return null;
    }
    //private AvroWriter writer;
    static int counter = 0;

    public void start(Map<String, String> map) {

        //writer = new AvroWriter();
        log.info("initialized the HDFS Sink ");
        this.context.timeout(2000);
    }

    public void put(Collection<SinkRecord> collection) {

        try {
           *//* Collection<String> recordsAsString = collection.stream().map(r -> String.valueOf(r.value()))
                    .collect(Collectors.toList());*//*
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
                ConnectSchema valSchema = (ConnectSchema)v;
                List<Field> fields = valSchema.fields();
                for(Field f:fields){
                    System.out.println("field name is-->"+f.name());
                    System.out.println("field schema is-->"+f.schema());
                }

                if (p != null) {
                    System.out.println("value Obtained");
                }

                Map<String,RecordSchema> records = new HashMap<String,RecordSchema>();

                Struct rec_st = (Struct)p;


                for(Field f:fields){
                    System.out.println("field name is-->"+f.name());
                    System.out.println("field schema is-->"+f.schema());

                    Object rec_val = rec_st.get(f.name());
                    Schema rec_key = f.schema();
                    records.put(f.name(),new HSimpleTask.RecordSchema(rec_key,rec_val));

                }

                String namespace_tmp = valSchema.name();
                String namespace = namespace_tmp.substring(0,namespace_tmp.lastIndexOf("."));
                System.out.println("The namespace is-->"+namespace);
                String name = namespace_tmp.substring(namespace_tmp.lastIndexOf(".")+1,namespace_tmp.length());

                System.out.println("The name of the avro record-->"+name);

                org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> field_tmp =  org.apache.avro.SchemaBuilder.record(name).namespace(namespace)
                        .fields();

                for(Map.Entry<String,RecordSchema> r:records.entrySet()){
                    String valSc = r.getValue().getTypeInfo().toString();
                    switch (valSc){
                        case "STRING_SCHEMA":
                            field_tmp =  field_tmp.name(r.getKey()).type().stringType().noDefault();
                            break;
                        case "FLOAT32_SCHEMA":
                            field_tmp =  field_tmp.name(r.getKey()).type().stringType().noDefault();
                            break;

                        default:
                    }
                }

                org.apache.avro.Schema final_schema = field_tmp.endRecord();


                GenericRecord customerRecord = new GenericData.Record(final_schema);
                for(Map.Entry<String,RecordSchema> r:records.entrySet()){

                    customerRecord.put(r.getKey(),r.getValue().getValue());
                }



                //log.info("The record schema is -->"+v.);
                log.info("The record class is --> " + p.getClass());
                log.info("The record is --> " + p);
                log.info("The record is --> " + p.toString());

                Struct record = (Struct) p;


                Schema dateSchema = SchemaBuilder.struct()
                        .name("com.example.CalendarDate").version(2).doc("A calendar date including month, day, and year.")
                        .field("month", Schema.STRING_SCHEMA)
                        .field("day", Schema.INT8_SCHEMA)
                        .field("year", Schema.INT16_SCHEMA)

                        .build();

                Schema dateSchema2 = SchemaBuilder.struct()
                        .name("com.example.CalendarDate").version(2).doc("A calendar date including month, day, and year.")
                        .field("month", Schema.STRING_SCHEMA)
                        .field("day", Schema.INT8_SCHEMA)
                        .field("year2", dateSchema)

                        .build();



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
*/