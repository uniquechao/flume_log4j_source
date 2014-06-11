/**
 * com.xschao.kafka.StringSplitTest
 * xushichao
 * 2014年3月8日
 */
package com.xschao.kafka;

/**
 * @author xushichao
 * @date 2014年3月8日
 */
public class StringSplitTest {
	public static void main(final String[] args) {
		String str = "the sky is blue";
		reverser(str);
	}

	private static void reverser(final String str) {
		String[] splits = str.split(" ");
		for (int i = splits.length - 1; i >= 0; i--) {
			System.out.print(splits[i] + " ");
		}
	}

}
