package com.harish.kafka.streams.config;

import com.harish.kafka.streams.dto.Document;

import java.util.ArrayList;
import java.util.List;

public class DocumentListAccumulator {
    public List<Document> getList() {
        return list;
    }

    public void setList(List<Document> list) {
        this.list = list;
    }

    private List<Document> list = new ArrayList<>();

    public DocumentListAccumulator add(Document s) {
        list.add(s);
        return this;
    }
}
