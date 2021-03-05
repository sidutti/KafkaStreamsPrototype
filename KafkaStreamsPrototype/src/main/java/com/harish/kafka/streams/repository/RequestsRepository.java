package com.harish.kafka.streams.repository;

import com.harish.kafka.streams.dto.Requests;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RequestsRepository extends ReactiveMongoRepository<Requests, String> {

}
