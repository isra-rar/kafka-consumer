package com.consumer.consumer;

import com.consumer.DTO.CarDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CarConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CarConsumer.class);

    @Value(value = "${topic.name}")
    private String topic;

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String groupId;

    @KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "carKafkaListenerContainerFactory")
    public void listenTopicCar(ConsumerRecord<String, CarDTO> record) {
        logger.info("Received Message " + record.partition());
        logger.info("Received Message " + record.topic());
        logger.info("Received Message " + record.key());
        logger.info("Received Message " + record.value());
    }

}
