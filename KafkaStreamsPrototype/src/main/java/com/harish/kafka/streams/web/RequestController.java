package com.harish.kafka.streams.web;

import com.harish.kafka.streams.dto.Document;
import com.harish.kafka.streams.dto.Operation;
import com.harish.kafka.streams.dto.Requests;
import com.harish.kafka.streams.repository.RequestsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class RequestController {

    private final KafkaTemplate<String, Requests> kafkaTemplate;
    private final RequestsRepository requestsRepository;

    @Autowired
    public RequestController(KafkaTemplate<String, Requests> kafkaTemplate, RequestsRepository requestsRepository) {
        this.requestsRepository = requestsRepository;
        kafkaTemplate.setMessageConverter(new JsonMessageConverter());
        this.kafkaTemplate = kafkaTemplate;
    }


    @GetMapping(value = "/request/{request}")
    public Flux<Requests> createRequestEvents(@PathVariable(name = "request") Integer count) {
        Requests requests = new Requests();
        List<Document> documentList = Stream
                .iterate(0, i -> i + 1)
                .limit(count)
                .map(i -> {
                    Document document = new Document();
                    document.setGuid(requests.getGuid());
                    document.setOperation(Operation.values()[i % 3]);
                    return document;
                })
                .collect(Collectors.toList());
        requests.setDocumentsList(documentList);
        //TestData
        //TS-->RS MQ
        kafkaTemplate.send("request-events", requests.getGuid(), requests).isDone();
        return requestsRepository.findAll();
    }
    @GetMapping(value = "/request/delete")
    public Mono<Void> deleteRequests(){
        return requestsRepository.deleteAll();
    }
    @GetMapping(value = "/request/guids")
    public Mono<List<String>> getGuids(){
        return requestsRepository.findAll()
                .map(Requests::getGuid)
                .collect(Collectors.toList());
    }
}
