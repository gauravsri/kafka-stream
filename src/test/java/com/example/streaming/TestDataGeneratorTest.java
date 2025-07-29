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
@DisplayName("TestDataGenerator Tests")
class TestDataGeneratorTest {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Test
    @DisplayName("Should create test message with default values")
    void shouldCreateTestMessageWithDefaultValues() {
        Message message = TestDataGenerator.createTestMessage();

        assertThat(message.getMessageType()).isNotNull();
        assertThat(message.getDate()).isEqualTo(LocalDate.now().format(DATE_FORMATTER));
        assertThat(message.getMessageId()).isNotNull();
        assertThat(message.getCreationTime()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should create test message with specified parameters")
    void shouldCreateTestMessageWithSpecifiedParameters() {
        String messageType = "test_event";
        String date = "2024-01-15";

        Message message = TestDataGenerator.createTestMessage(messageType, date);

        assertThat(message.getMessageType()).isEqualTo(messageType);
        assertThat(message.getDate()).isEqualTo(date);
        assertThat(message.getMessageId()).isNotNull();
        assertThat(message.getCreationTime()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should create multiple test messages")
    void shouldCreateMultipleTestMessages() {
        int count = 5;

        List<Message> messages = TestDataGenerator.createTestMessages(count);

        assertThat(messages).hasSize(count);
        assertThat(messages.stream().map(Message::getMessageId)).doesNotHaveDuplicates();
        assertThat(messages.stream().map(Message::getCreationTime)).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("Should create test messages with specified types")
    void shouldCreateTestMessagesWithSpecifiedTypes() {
        String[] messageTypes = {"user_event", "system_event", "error_event"};

        List<Message> messages = TestDataGenerator.createTestMessagesWithTypes(messageTypes);

        assertThat(messages).hasSize(messageTypes.length);
        assertThat(messages.stream().map(Message::getMessageType))
                .containsExactly(messageTypes);
    }

    @Test
    @DisplayName("Should create test messages with specified dates")
    void shouldCreateTestMessagesWithSpecifiedDates() {
        String[] dates = {"2024-01-15", "2024-01-16", "2024-01-17"};

        List<Message> messages = TestDataGenerator.createTestMessagesWithDates(dates);

        assertThat(messages).hasSize(dates.length);
        assertThat(messages.stream().map(Message::getDate))
                .containsExactly(dates);
    }

    @Test
    @DisplayName("Should serialize message to byte array")
    void shouldSerializeMessageToByteArray() throws IOException {
        Message message = TestDataGenerator.createTestMessage("test_event", "2024-01-15");

        byte[] serialized = TestDataGenerator.serializeMessage(message);

        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should serialize multiple messages")
    void shouldSerializeMultipleMessages() throws IOException {
        List<Message> messages = TestDataGenerator.createTestMessages(3);

        List<byte[]> serializedMessages = TestDataGenerator.serializeMessages(messages);

        assertThat(serializedMessages).hasSize(3);
        assertThat(serializedMessages).allMatch(bytes -> bytes.length > 0);
    }

    @Test
    @DisplayName("Should generate unique message IDs")
    void shouldGenerateUniqueMessageIds() {
        List<Message> messages = TestDataGenerator.createTestMessages(100);

        List<String> messageIds = messages.stream()
                .map(message -> message.getMessageId().toString())
                .collect(java.util.stream.Collectors.toList());

        assertThat(messageIds).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("Should generate creation times within reasonable range")
    void shouldGenerateCreationTimesWithinReasonableRange() {
        long startTime = System.currentTimeMillis();
        
        List<Message> messages = TestDataGenerator.createTestMessages(10);
        
        long endTime = System.currentTimeMillis() + 100; // Allow some buffer for incremental timestamps

        assertThat(messages.stream().map(Message::getCreationTime))
                .allMatch(time -> time >= startTime && time <= endTime);
    }

    @Test
    @DisplayName("Should handle empty arrays gracefully")
    void shouldHandleEmptyArraysGracefully() {
        List<Message> messagesWithTypes = TestDataGenerator.createTestMessagesWithTypes();
        List<Message> messagesWithDates = TestDataGenerator.createTestMessagesWithDates();

        assertThat(messagesWithTypes).isEmpty();
        assertThat(messagesWithDates).isEmpty();
    }

    @Test
    @DisplayName("Should serialize and maintain data integrity")
    void shouldSerializeAndMaintainDataIntegrity() throws IOException {
        Message originalMessage = TestDataGenerator.createTestMessage("integrity_test", "2024-01-15");
        
        byte[] serialized = TestDataGenerator.serializeMessage(originalMessage);
        
        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(0);
        
        assertThat(originalMessage.getMessageType()).isEqualTo("integrity_test");
        assertThat(originalMessage.getDate()).isEqualTo("2024-01-15");
    }
}