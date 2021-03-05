package com.harish.kafka.streams.service;

import com.harish.kafka.streams.dto.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OperationsRouter {
    private final List<CrudService> crudServices;

    @Autowired
    public OperationsRouter(List<CrudService> crudServices) {
        this.crudServices = crudServices;
    }

    public Document process(Document document) {
            return crudServices
                    .stream()
                    .filter(crudService -> crudService.canI(document.getOperation()))
                    .findFirst()
                    .get()
                    .process(document);

    }
}
