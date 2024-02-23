package io.streams.kstreamsbankapplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streams.kstreamsbankapplication.dto.BankTransaction;
import io.streams.kstreamsbankapplication.service.BankProducerService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class BankProducerServiceTest {

    @Autowired
    private BankProducerService bankProducerService;

    @Test
    void testNewRandomTransaction() throws JsonProcessingException {
        ProducerRecord<String, String> producerRecord = bankProducerService.newRandomTransaction("John");
        String key = producerRecord.key();
        String value = producerRecord.value();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            BankTransaction bankTransaction = objectMapper.readValue(value, BankTransaction.class);
            assertTrue(bankTransaction.getAmount() < 100, "Amount Should be less than 100!");
            assertEquals("John", bankTransaction.getName());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        assertEquals("John", key);
    }
}