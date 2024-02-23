package io.streams.kstreamsbankapplication.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streams.kstreamsbankapplication.dto.BankTransaction;
import io.streams.kstreamsbankapplication.dto.TransactionType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class BankProducerService {

    private final Logger logger = LoggerFactory.getLogger(BankProducerService.class);

    private final KafkaProducer<String, String> kafkaProducer;

    private final String topicName;

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    public BankProducerService(KafkaProducer<String, String> kafkaProducer,
                               @Value("${kafka.input.topic.name}") String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    @Async
    public void produceTransactionRecords() {
        int i = 0;
        while (i < 1) {
            logger.info(String.format("Producing batch - %d", i));
            try {
                kafkaProducer.send(newRandomTransaction("John")).get();
                Thread.sleep(100);
                kafkaProducer.send(newRandomTransaction("Bob")).get();
                Thread.sleep(100);
                kafkaProducer.send(newRandomTransaction("Alice")).get();
                Thread.sleep(100);
                i = i + 1 ;
            } catch (InterruptedException | JsonProcessingException e) {
                logger.warn("Interrupted!", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.warn("Execution Exception", e);
            }
        }
    }

    public ProducerRecord<String, String> newRandomTransaction(String name) throws JsonProcessingException {
        Instant instant = Instant.now();
        int amount = ThreadLocalRandom.current().nextInt(-100, 100);
        BankTransaction bankTransaction = new BankTransaction();
        bankTransaction.setName(name);
        bankTransaction.setAmount(amount);
        bankTransaction.setTimestamp(instant.toString());
        if(amount >= 0) {
            bankTransaction.setTransactionType(TransactionType.DEPOSIT);
        } else {
            bankTransaction.setTransactionType(TransactionType.WITHDRAW);
        }
        return new ProducerRecord<>(topicName, name, toJson(bankTransaction));
    }

    private static String toJson(BankTransaction bankTransaction) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(bankTransaction);
    }
}
