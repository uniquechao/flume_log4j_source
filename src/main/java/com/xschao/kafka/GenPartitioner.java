/**
 * com.xschao.kafka.GenPartitioner
 * kongying
 * 2014年3月17日
 */
package com.xschao.kafka;

import java.util.Random;

import kafka.producer.Partitioner;

import org.apache.log4j.Logger;

/**
 * 
 * @author kongying
 * @date 2014年3月17日
 */

public class GenPartitioner implements Partitioner {
	public static final Logger LOGGER = Logger
			.getLogger(ProducerPartitioner.class);
	private static Random random = new Random();

	@Override
	public int partition(final Object key, final int numPartitions) {
		LOGGER.info("ProducerPartitioner key:" + key + " partitions:"
				+ numPartitions);
		if (key == null) {
			return random.nextInt(numPartitions);
		}
		return Math.abs(key.hashCode()) % numPartitions;
	}
}