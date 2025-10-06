package com.jpmc.midascore.kafka;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.jpmc.midascore.foundation.Transaction; // <-- correct package

@Service
public class TransactionListener {
    private static final Logger log = LoggerFactory.getLogger(TransactionListener.class);
    private final AtomicInteger seen = new AtomicInteger(0);

    // topic from application.yml -> general.kafka-topic
    @KafkaListener(topics = "${general.kafka-topic}")
    public void onMessage(@Payload Transaction tx) {
        log.debug("Received tx: {}", tx);
        int n = seen.incrementAndGet();
        if (n <= 4) {
            System.out.println("FIRST4 amount=" + tx.getAmount());
        }
    }
}
