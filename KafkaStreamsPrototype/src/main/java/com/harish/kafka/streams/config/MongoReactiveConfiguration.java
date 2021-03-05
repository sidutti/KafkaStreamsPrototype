package com.harish.kafka.streams.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

@Configuration
@EnableReactiveMongoRepositories(basePackages = "com.harish.kafka.streams.repository")
public class MongoReactiveConfiguration extends AbstractReactiveMongoConfiguration {
    @Override
    protected String getDatabaseName() {
        return "local";
    }

    @Bean
    public MongoClient reactiveMongoClient() {
        return MongoClients.create("mongodb://192.168.1.99:27017/?minpoolsize=10&connecttimeoutms=6000000&sockettimeoutms=600000&maxlifetimems=600000");
    }
    protected void configureClientSettings(MongoClientSettings.Builder builder) {
        builder.applicationName("sidutti");
        ConnectionString connectionString =  new ConnectionString("mongodb://192.168.1.99:27017/?minpoolsize=10&connecttimeoutms=6000000&sockettimeoutms=600000&maxlifetimems=600000");

        builder.applyConnectionString(connectionString);
        // customization hook
    }
/*
    public @Bean
    ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory() {
        return new SimpleReactiveMongoDatabaseFactory(MongoClients.create("mongodb://192.168.1.99:27017"), "database");
    }
*/


  /*  public @Bean
    ReactiveMongoTemplate reactiveMongoTemplate() {
        return new ReactiveMongoTemplate(reactiveMongoDatabaseFactory());
    }*/
}
