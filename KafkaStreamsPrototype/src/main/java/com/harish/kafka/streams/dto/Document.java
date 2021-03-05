package com.harish.kafka.streams.dto;

import java.util.HashMap;
import java.util.Map;

public class Document {

    private String guid;
    private String docId;
    private Map<String, String> properties;
    private Operation operation;

    public Document() {

    }

    public Document(String docId, Map<String, String> properties, Operation operation) {
        this.docId = docId;
        this.properties = properties;
        this.operation = operation;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public Map<String, String> getProperties() {
        if(properties==null){
            this.properties = new HashMap<>();
        }
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }
    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

}
