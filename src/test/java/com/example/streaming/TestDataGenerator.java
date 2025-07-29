package com.example.streaming;

import com.example.avro.Message;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestDataGenerator {
    
    private static final Random random = new Random();
    private static final String[] MESSAGE_TYPES = {"user_event", "system_event", "error_event", "audit_event"};
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public static Message createTestMessage() {
        return createTestMessage(getRandomMessageType(), LocalDate.now().format(DATE_FORMATTER));
    }
    
    public static Message createTestMessage(String messageType, String date) {
        return Message.newBuilder()
                .setMessageType(messageType)
                .setDate(date)
                .setMessageId(UUID.randomUUID().toString())
                .setCreationTime(System.currentTimeMillis())
                .build();
    }
    
    public static List<Message> createTestMessages(int count) {
        List<Message> messages = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        
        for (int i = 0; i < count; i++) {
            Message message = Message.newBuilder()
                    .setMessageType(getRandomMessageType())
                    .setDate(LocalDate.now().format(DATE_FORMATTER))
                    .setMessageId(UUID.randomUUID().toString())
                    .setCreationTime(baseTime + i) // Ensure unique timestamps
                    .build();
            messages.add(message);
        }
        return messages;
    }
    
    public static List<Message> createTestMessagesWithTypes(String... messageTypes) {
        List<Message> messages = new ArrayList<>();
        String date = LocalDate.now().format(DATE_FORMATTER);
        long baseTime = System.currentTimeMillis();
        
        for (int i = 0; i < messageTypes.length; i++) {
            Message message = Message.newBuilder()
                    .setMessageType(messageTypes[i])
                    .setDate(date)
                    .setMessageId(UUID.randomUUID().toString())
                    .setCreationTime(baseTime + i) // Ensure unique timestamps
                    .build();
            messages.add(message);
        }
        return messages;
    }
    
    public static List<Message> createTestMessagesWithDates(String... dates) {
        List<Message> messages = new ArrayList<>();
        String messageType = getRandomMessageType();
        long baseTime = System.currentTimeMillis();
        
        for (int i = 0; i < dates.length; i++) {
            Message message = Message.newBuilder()
                    .setMessageType(messageType)
                    .setDate(dates[i])
                    .setMessageId(UUID.randomUUID().toString())
                    .setCreationTime(baseTime + i) // Ensure unique timestamps
                    .build();
            messages.add(message);
        }
        return messages;
    }
    
    public static byte[] serializeMessage(Message message) throws IOException {
        DatumWriter<Message> datumWriter = new SpecificDatumWriter<>(Message.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        
        datumWriter.write(message, encoder);
        encoder.flush();
        outputStream.close();
        
        return outputStream.toByteArray();
    }
    
    public static List<byte[]> serializeMessages(List<Message> messages) throws IOException {
        List<byte[]> serializedMessages = new ArrayList<>();
        for (Message message : messages) {
            serializedMessages.add(serializeMessage(message));
        }
        return serializedMessages;
    }
    
    private static String getRandomMessageType() {
        return MESSAGE_TYPES[random.nextInt(MESSAGE_TYPES.length)];
    }
}