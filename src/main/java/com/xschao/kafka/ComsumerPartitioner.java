/**
 * com.xschao.kafka.ComsumerPartitioner
 * kongying
 * 2014年3月17日
 */
package com.xschao.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 
 * @author kongying
 * @date 2014年3月17日
 */
public class ComsumerPartitioner extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;

	public static void main(final String[] args) {
		ComsumerPartitioner consumerPartitioner = new ComsumerPartitioner(
				"test");
		consumerPartitioner.start();
	}

	public ComsumerPartitioner(final String topic) {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "10.10.0.204:2181");
		props.put("group.id", "0");
		props.put("zookeeper.session.timeout.ms", "100000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);

	}

	@Override
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println(new String(it.next().message()));
		}
	}
}
