package com.harish.kafka.streams.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.harish.kafka.streams.dto.Document;
import com.harish.kafka.streams.dto.Requests;
import com.harish.kafka.streams.repository.RequestsRepository;
import com.harish.kafka.streams.service.OperationsRouter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
@EnableKafkaStreams
@EnableKafka
public class KafkaStreamsConfig {
    private final OperationsRouter operationsRouter;
    final JsonSerializer<Requests> requestsSerializer = new JsonSerializer<>();
    final JsonDeserializer<Requests> requestsDeserializer = new JsonDeserializer<>();
    final Serde<Requests> requestsSerde = Serdes.serdeFrom(requestsSerializer, requestsDeserializer);
    final JsonSerializer<Document> documentSerializer = new JsonSerializer<>();
    final JsonDeserializer<Document> documentDeserializer = new JsonDeserializer<>();
    final Serde<Document> documentSerde = Serdes.serdeFrom(documentSerializer, documentDeserializer);
    JsonSerializer<DocumentListAccumulator> accSerializer = new JsonSerializer<>();
    JsonDeserializer<DocumentListAccumulator> accDeserializer = new JsonDeserializer<>(DocumentListAccumulator.class);
    Serde<DocumentListAccumulator> accSerde = Serdes.serdeFrom(accSerializer, accDeserializer);

    private final RequestsRepository requestsRepository;

    @Autowired
    public KafkaStreamsConfig(OperationsRouter operationsRouter, RequestsRepository requestsRepository) {
        this.operationsRouter = operationsRouter;
        this.requestsRepository = requestsRepository;
        requestsSerializer.getTypeMapper().addTrustedPackages("*");
        requestsDeserializer.getTypeMapper().addTrustedPackages("*");
        documentSerializer.getTypeMapper().addTrustedPackages("*");
        documentDeserializer.getTypeMapper().addTrustedPackages("*");
    }

    @Bean
    public KStream<String, Requests> requestEvents(StreamsBuilder streamsBuilder) {
        //1 event
        KStream<String, Requests> requests = streamsBuilder
                .stream("request-events", Consumed.with(Serdes.String(), requestsSerde));
        //n event
        requests.flatMapValues(Requests::getDocumentsList)
                .to("document-events", Produced.with(Serdes.String(), new JsonSerde<>()));
        return requests;
    }

    @Bean
    public KStream<String, Document> documentEvents(StreamsBuilder streamsBuilder) {
        KStream<String, Document> documents = streamsBuilder
                .stream("document-events", Consumed.with(Serdes.String(), documentSerde));
        documents.mapValues(operationsRouter::process)
                .to("document-response", Produced.with(Serdes.String(), new JsonSerde<>()));
        return documents;
    }

    @Bean
    public KTable<String, DocumentListAccumulator> documentResponseEvents(StreamsBuilder streamsBuilder) {

        StoreBuilder<KeyValueStore<String,String>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("documentListTransformState"),
                        Serdes.String(),
                        Serdes.String());

        streamsBuilder.addStateStore(keyValueStoreBuilder);


        KTable<String, DocumentListAccumulator> aggregate =
                streamsBuilder
                .stream("document-response", Consumed.with(Serdes.String(), documentSerde))
                .groupBy((key, document) -> document.getGuid(),Grouped.with(Serdes.String(),documentSerde))
                .aggregate(DocumentListAccumulator::new,
                        (k, v, list) -> list.add(v),
                        Materialized.with(Serdes.String(),accSerde)
                );
        //break or condition
        aggregate.mapValues((readOnlyKey, value) -> {
            Requests requests = new Requests();
            requests.setDocumentsList(value.getList());
            requests.setGuid(requests.getDocumentsList().get(0).getGuid());
            requestsRepository.save(requests).block();
            return value;
        });

        return aggregate;
    }


    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
