package io.streams.kstreamsbankapplication;

import com.fasterxml.jackson.databind.JsonNode;
import io.streams.kstreamsbankapplication.dto.BankBalance;
import io.streams.kstreamsbankapplication.dto.BankTransaction;
import io.streams.kstreamsbankapplication.dto.BankTransactionStatus;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@EnableKafkaStreams
public class BankBalanceStream {

    private final KafkaStreamsConfiguration kStreamsConfig;

    private final String inputTopicName;
    private final String outputTopicName;
    private final String rejectedTransactionsTopic;

    private static final Serde<String> STRING_SERDES = Serdes.String();

    @Autowired
    public BankBalanceStream(@Qualifier("defaultKafkaStreamsConfig") KafkaStreamsConfiguration kStreamsConfig,
                             @Value("${kafka.input.topic.name}") String inputTopicName,
                             @Value("${kafka.output.topic.name}") String outputTopicName,
                             @Value("${kafka.output.rejected.topic.name}") String rejectedTransactionsTopic) {
        this.kStreamsConfig = kStreamsConfig;
        this.inputTopicName = inputTopicName;
        this.outputTopicName = outputTopicName;
        this.rejectedTransactionsTopic = rejectedTransactionsTopic;
    }

    @Autowired
    void buildTransactionsPipeline(StreamsBuilder streamsBuilder) {
        Serde<BankTransaction> bankTransactionSerdes = new JsonSerde<>(BankTransaction.class);
        Serde<BankBalance> bankBalanceSerde = new JsonSerde<>(BankBalance.class);
        KStream<String, BankTransaction> bankTransactions = streamsBuilder.stream(inputTopicName, Consumed.with(Serdes.String(), bankTransactionSerdes));

        KTable<String, BankBalance> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), bankTransactionSerdes))
                .aggregate(
                        BankBalance::new,
                        (key, bankTransaction, balance) -> newBalance(bankTransaction, balance),
                        Named.as("bank-balance-agg"),
                        Materialized.with(STRING_SERDES, bankBalanceSerde)
                );

        bankBalance.toStream().to(outputTopicName, Produced.with(STRING_SERDES, bankBalanceSerde));

        KTable<String, BankTransaction> rejectedTransactions = bankBalance.mapValues((readOnlyKey, value) -> value.getRecentTransaction())
                .filter((key, value) -> value.getStatus() == BankTransactionStatus.REJECTED);

        rejectedTransactions.toStream().to(rejectedTransactionsTopic, Produced.with(Serdes.String(), bankTransactionSerdes));

        try (KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kStreamsConfig.asProperties())) {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            // Add shutdown hook to handle shutdown gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }

    public static BankBalance newBalance(BankTransaction transaction, BankBalance balance) {
        BankBalance newBalance = new BankBalance();
        newBalance.setCount(balance.getCount() + 1);
        Integer currentBalance = balance.getBalance();
        Integer transactionAmount = transaction.getAmount();
        if(currentBalance > 0 && currentBalance >= transactionAmount) {
            newBalance.setBalance(balance.getBalance() + transaction.getAmount());
            transaction.setStatus(BankTransactionStatus.APPROVED);
        } else {
            newBalance.setBalance(balance.getBalance());
            transaction.setStatus(BankTransactionStatus.REJECTED);
        }
        newBalance.setTime(transaction.getTimestamp());
        newBalance.setRecentTransaction(transaction);
        return newBalance;
    }
}
