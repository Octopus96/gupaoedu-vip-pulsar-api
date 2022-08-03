package com.gupaoedu.mq.pulsar.javaapi;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 多订阅
 */
public class MultiConsumer {
    private static final Logger log = LoggerFactory.getLogger(MultiConsumer.class);

//    private static final String SERVER_URL = "pulsar://192.168.8.147:6650";

    private static final String SERVER_URL = "pulsar://192.168.100.101:6650,192.168.100.102:6650,192.168.100.103:6650";
    private static final String DEFAULT_NS_TOPICS = "persistent://public/default/.*";
    private static final String DEFATULT_NS_REG_TOPICS= "persistent://public/default/my.*";
    private static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVER_URL)
                .enableTcpNoDelay(true)
                .build();
        ConsumerBuilder consumerBuilder = client.newConsumer()
                .subscriptionName("multi-sub");

        // 订阅namespace下所有的topic
        Pattern allTopicsInNamespace = Pattern.compile(DEFAULT_NS_TOPICS);
        consumerBuilder.topicsPattern("").subscribe();

        // 订阅namespace下满足正则匹配的topic
        Pattern someTopicsInNamespace = Pattern.compile(DEFATULT_NS_REG_TOPICS);
        Consumer allTopicsConsumer = consumerBuilder
                .topicsPattern(someTopicsInNamespace)
                .subscribe();

        List<String> topics = Arrays.asList(
                "topic-1",
                "topic-2",
                "topic-3"
        );

        Consumer multiTopicConsumer = consumerBuilder
                .topics(topics)
                .subscribe();

    }
}