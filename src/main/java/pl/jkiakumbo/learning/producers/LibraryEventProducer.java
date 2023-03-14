package pl.jkiakumbo.learning.producers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import pl.jkiakumbo.learning.domain.LibraryEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@Service
@Slf4j
public class LibraryEventProducer {

    public static final String TOPIC = "library-events";
    public static final Integer PARTITION = null;
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(libraryEvent);
        CompletableFuture<SendResult<Integer, String>> sendResultCompletableFuture = kafkaTemplate.send(producerRecord);
        sendResultCompletableFuture.whenComplete(sendResultHandler);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        List<Header> recordHeaders = List.of(new RecordHeader("scanner", "library-scanner".getBytes()));
        return new ProducerRecord<>(TOPIC, PARTITION, key, value,recordHeaders);
    }

    private BiConsumer<SendResult<Integer, String>, Throwable> sendResultHandler = (result, throwable) -> {
        if(throwable != null){
            log.error("Error Sending the message and the exception is: {}", throwable.getMessage());
        } else {
            RecordMetadata metadata = result.getRecordMetadata();
            ProducerRecord<Integer, String> record = result.getProducerRecord();
            log.info("Message Sent SuccessFully for the key: {} and the value is: {}, to topic: {} in partition {}",
                    record.key(), record.value(), metadata.topic(), metadata.partition());
        }
    };


}
