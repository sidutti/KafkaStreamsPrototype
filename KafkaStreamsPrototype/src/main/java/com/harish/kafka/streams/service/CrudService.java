package com.harish.kafka.streams.service;

import com.harish.kafka.streams.dto.Document;
import com.harish.kafka.streams.dto.Operation;

public interface CrudService {
    boolean canI(Operation operation);

    Document process(Document document);
}
