package com.oldjim.concurrent.executor;

import java.util.concurrent.ConcurrentLinkedQueue;

public class OJThreadPoolExecutorTest {
	public static void main(String[] args) throws InterruptedException {
		OJThreadPoolExecutor threadPool = new OJThreadPoolExecutor(4,new ConcurrentLinkedQueue<Runnable>());
		for (int i = 0; i < 50; i++) {
			Runnable runnable = new Runnable() {
				public void run() {
					System.out.println("hh");
				}
			};
			threadPool.execute(runnable);
		}
		threadPool.shutdownNow();
		System.out.println(threadPool.isTerminated());
		Thread.sleep(5000);
		System.out.println(threadPool.isTerminated());
	}
}
