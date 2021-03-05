package com.harish.kafka.streams.service;

import com.harish.kafka.streams.dto.Document;
import com.harish.kafka.streams.dto.Operation;
import org.springframework.stereotype.Component;

@Component
public class SearchService implements CrudService {
    @Override
    public boolean canI(Operation operation) {
        return operation.equals(Operation.SEARCH);
    }

    @Override
    public Document process(Document document) {
        document.getProperties().put("Operation", Operation.SEARCH.name());
        return document;
    }
}
