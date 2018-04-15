package com.app.partitioner;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by samgupta0 on 4/15/2018.
 */
public class HSimpleConnector extends SinkConnector {

    private Map<String, String> configProperties;
    private String topic;
    private String partitions;
    private String  fileSystemName;

    public String version() {
        return "1";
    }

    public void start(Map<String, String> map) {


        try {
           this.topic = map.get("topic");
           this.partitions = map.get("partitions");
            String fileSystemName = "fileSystemName";
            this.fileSystemName = map.get(fileSystemName);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start Hdfs Sink Connector due to configuration error",e);
        }
    }

    public Class<? extends Task> taskClass() {
        return HCsvTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {

        List<Map<String,String>> taskConfigs = new ArrayList<Map<String,String>>();

        for(int i=0;i<this.partitions.length();i++){

            Map<String,String> taskProps = new HashMap<String,String>();
            taskProps.put("topic",this.topic);
            taskProps.put("partition",String.valueOf(i));
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    public void stop() {

    }

    @Override
    public ConfigDef config() {
        ConfigDef defs = new ConfigDef();
        return defs;
    }
}
