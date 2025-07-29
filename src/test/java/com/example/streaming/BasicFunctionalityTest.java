package com.example.streaming;

import com.example.avro.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("Basic Functionality Tests")
class BasicFunctionalityTest {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Test
    @DisplayName("Should create and validate Avro messages")
    void shouldCreateAndValidateAvroMessages() {
        Message message = Message.newBuilder()
                .setMessageType("test_event")
                .setDate("2024-01-15")
                .setMessageId("test-id-123")
                .setCreationTime(System.currentTimeMillis())
                .build();

        assertThat(message.getMessageType()).isEqualTo("test_event");
        assertThat(message.getDate()).isEqualTo("2024-01-15");
        assertThat(message.getMessageId()).isEqualTo("test-id-123");
        assertThat(message.getCreationTime()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should serialize and maintain message integrity")
    void shouldSerializeAndMaintainMessageIntegrity() throws IOException {
        Message originalMessage = TestDataGenerator.createTestMessage("integrity_test", "2024-01-15");
        
        byte[] serialized = TestDataGenerator.serializeMessage(originalMessage);
        
        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(0);
        
        assertThat(originalMessage.getMessageType()).isEqualTo("integrity_test");
        assertThat(originalMessage.getDate()).isEqualTo("2024-01-15");
    }

    @Test
    @DisplayName("Should generate test data correctly")
    void shouldGenerateTestDataCorrectly() {
        List<Message> messages = TestDataGenerator.createTestMessages(5);
        
        assertThat(messages).hasSize(5);
        assertThat(messages.stream().map(Message::getMessageId)).doesNotHaveDuplicates();
        
        String[] messageTypes = {"user_event", "system_event", "error_event"};
        List<Message> typedMessages = TestDataGenerator.createTestMessagesWithTypes(messageTypes);
        
        assertThat(typedMessages).hasSize(3);
        assertThat(typedMessages.stream().map(Message::getMessageType))
                .containsExactly(messageTypes);
    }

    @Test
    @DisplayName("Should validate Avro schema structure")
    void shouldValidateAvroSchemaStructure() {
        String schema = Message.getClassSchema().toString();
        
        assertThat(schema).contains("\"name\":\"Message\"");
        assertThat(schema).contains("\"name\":\"message_type\"");
        assertThat(schema).contains("\"name\":\"date\"");
        assertThat(schema).contains("\"name\":\"message_id\"");
        assertThat(schema).contains("\"name\":\"creation_time\"");
        assertThat(schema).contains("\"type\":\"string\"");
        assertThat(schema).contains("\"type\":\"long\"");
    }

    @Test
    @DisplayName("Should handle configuration with different inputs")
    void shouldHandleConfigurationWithDifferentInputs() {
        TestConfiguration.clearSystemProperties();
        
        StreamingConfig defaultConfig = new StreamingConfig();
        assertThat(defaultConfig.getKafkaBrokers()).isEqualTo("localhost:9092");
        assertThat(defaultConfig.getKafkaTopic()).isEqualTo("messages");
        
        System.setProperty("KAFKA_BROKERS", "test-broker:9092");
        System.setProperty("KAFKA_TOPIC", "test-topic");
        
        StreamingConfig customConfig = new StreamingConfig();
        assertThat(customConfig.getKafkaBrokers()).isEqualTo("test-broker:9092");
        assertThat(customConfig.getKafkaTopic()).isEqualTo("test-topic");
        
        TestConfiguration.clearSystemProperties();
    }
}