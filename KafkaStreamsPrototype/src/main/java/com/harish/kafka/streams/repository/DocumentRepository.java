package com.harish.kafka.streams.repository;

import com.harish.kafka.streams.dto.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DocumentRepository extends ReactiveMongoRepository<Document, String> {
}
