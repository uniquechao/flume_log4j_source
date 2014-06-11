/**
 * com.xschao.kafka.ClassProducerTest
 * xushichao
 * 2014年2月25日
 */
package com.xschao.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka测试程序Producer
 * 
 * @author xushichao
 * @date 2014年2月25日
 */
public class ClassProducerTest {
	public static void main(final String[] args) {
		Properties props = new Properties();
		props.setProperty("metadata.broker.list",
				"10.10.1.202:9096,10.10.1.202:9097");

		props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				"test", "test-duan zhi lang");
		try {
			producer.send(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.close();
	}
}
