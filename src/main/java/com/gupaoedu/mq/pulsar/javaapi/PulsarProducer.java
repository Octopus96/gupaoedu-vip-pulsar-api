package com.gupaoedu.mq.pulsar.javaapi;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PulsarProducer {
    private static final Logger log = LoggerFactory.getLogger(PulsarProducer.class);
//    private static final String SERVER_URL = "pulsar://192.168.8.147:6650";

    private static final String SERVER_URL = "pulsar://192.168.100.101:6650,192.168.100.102:6650,192.168.100.103:6650";
    public static void main(String[] args) throws Exception {
        // 构造Pulsar Client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVER_URL)
                .enableTcpNoDelay(true)
                .build();
        // 构造生产者
        Producer<String> producer = client.newProducer(Schema.STRING)
                .producerName("my-producer")
                .topic("persistent://public/default/my-topic")
                .batchingMaxMessages(1024)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .enableBatching(true)
                .blockIfQueueFull(true)
                .maxPendingMessages(512)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
        // 同步发送消息
        MessageId messageId = producer.send("Hello World");
        log.info("message id is {}",messageId);
        CompletableFuture<MessageId> asyncMessageId = producer.sendAsync("This is a async message");
        // 阻塞线程，直到返回结果
        log.info("async message id is {}",asyncMessageId.get());

        // 配置发送的消息元信息，同步发送
        producer.newMessage()
                .key("my-message-key")
                .value("my-message")
                .property("my-key", "my-value")
                .property("my-other-key", "my-other-value")
                .send();
        producer.newMessage()
                .key("my-async-message-key")
                .value("my-async-message")
                .property("my-async-key", "my-async-value")
                .property("my-async-other-key", "my-async-other-value")
                .sendAsync();

        // 关闭producer的方式有两种：同步和异步
        // producer.closeAsync();
        producer.close();

        // 关闭licent的方式有两种，同步和异步
        // client.close();
        client.closeAsync();

    }
}