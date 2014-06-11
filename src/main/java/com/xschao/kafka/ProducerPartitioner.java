/**
 * com.xschao.kafka.ProducerPartitioner
 * kongying
 * 2014年3月17日
 */
package com.xschao.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author kongying
 * @date 2014年3月17日
 */

public class ProducerPartitioner {

	public static void main(final String[] args) {
		Properties props = new Properties();
		props.setProperty("metadata.broker.list",
				"10.10.1.202:9096,10.10.1.202:9097");
		props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		// 设置分区信息
		// props.put("partitioner.class", "com.xschao.kafka.GenPartitioner");
		props.setProperty("partitioner.class", "com.xschao.kafka.GenPartitioner");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		// 中间值为ProducerPartitioner中的key
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				"test", "partition", "test-duan zhi lang");
		try {
			producer.send(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.close();
	}
}
