package com.harish.kafka.streams.service;

import com.harish.kafka.streams.dto.Document;
import com.harish.kafka.streams.dto.Operation;
import org.springframework.stereotype.Component;

@Component
public class ReadService implements CrudService {
    @Override
    public boolean canI(Operation operation) {
        return operation.equals(Operation.READ);
    }

    @Override
    public Document process(Document document) {
        document.getProperties().put("Operation", Operation.READ.name());
        return document;
    }
}
