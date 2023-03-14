package pl.jkiakumbo.learning;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import pl.jkiakumbo.learning.domain.Book;
import pl.jkiakumbo.learning.domain.LibraryEvent;
import pl.jkiakumbo.learning.domain.LibraryEventType;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerIntegrationTest {

    public static final int LIBRARY_EVENT_ID = 1;
    public static final int BOOK_ID = 1;
    public static final String URL = "/events";
    public static final String GROUP = "group1";
    public static final String TOPIC = "library-events";
    public static final String AUTO_COMMIT = "true";
    public static final String BOOK_NAME = "Kafka with Spring Boot 3";
    public static final String AUTHOR = "João Kiakumbo";
    public static final String HEADER_NAME = "content-type";
    private TestRestTemplate restTemplate;
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    private ObjectMapper objectMapper;

    @Autowired
    public LibraryEventControllerIntegrationTest(TestRestTemplate restTemplate, EmbeddedKafkaBroker embeddedKafkaBroker) {
        this.restTemplate = restTemplate;
        this.embeddedKafkaBroker = embeddedKafkaBroker;
    }

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps(GROUP, AUTO_COMMIT, embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldPostLibraryEvent() throws JsonProcessingException {
        // GIVEN
        Book book = new Book(BOOK_ID, BOOK_NAME, AUTHOR);
        LibraryEvent libraryEvent = new LibraryEvent(LIBRARY_EVENT_ID, LibraryEventType.NEW, book);
        HttpHeaders headers = new HttpHeaders();
        headers.set(HEADER_NAME, MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent);

        LibraryEvent expected = libraryEvent.setType(LibraryEventType.NEW);

        // WHEN
        ResponseEntity<LibraryEvent> response = restTemplate.exchange(URL, HttpMethod.POST, request, LibraryEvent.class);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, TOPIC);

        // THEN
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(objectMapper.writeValueAsString(expected), consumerRecord.value());
    }
}
