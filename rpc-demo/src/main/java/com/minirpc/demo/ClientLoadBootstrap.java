package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientLoadBootstrap {
    public static void main(String[] args) throws InterruptedException {
        // 并发线程数：模拟同时发起请求的调用方数量。
        int threadCount = 8;
        // 每个线程发送的请求数。
        int requestsPerThread = 20;
        // 复用同一个 RpcClient，验证 Day3 连接复用和请求映射逻辑。
        RpcClient rpcClient = new RpcClient("127.0.0.1", 9000);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
            HelloService helloService = rpcClientProxy.create(HelloService.class);

            // 主线程等待所有并发任务结束。
            CountDownLatch latch = new CountDownLatch(threadCount);
            // 线程安全错误集合，记录压测过程中捕获到的异常。
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
            long startNanos = System.nanoTime();
            for (int t = 0; t < threadCount; t++) {
                int threadIndex = t;
                executorService.submit(() -> {
                    try {
                        for (int i = 0; i < requestsPerThread; i++) {
                            // 每次调用都会走 requestId -> future 映射配对流程。
                            String result = helloService.hello("load-" + threadIndex + "-" + i);
                            if (result == null || result.isBlank()) {
                                errors.add(new IllegalStateException("响应为空"));
                            }
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            // 读取 Day3 新增的客户端统计指标。
            RpcClient.ClientStats stats = rpcClient.getStats();
            System.out.println("load test done, durationMs=" + durationMillis);
            System.out.println("stats total=" + stats.totalRequests()
                    + ", success=" + stats.successRequests()
                    + ", failed=" + stats.failedRequests()
                    + ", timeout=" + stats.timeoutRequests());
            if (!errors.isEmpty()) {
                throw new RuntimeException("压测存在异常，数量=" + errors.size(), errors.get(0));
            }
        } finally {
            // 收尾：关闭线程池和 RpcClient（连接、eventLoop）。
            executorService.shutdownNow();
            rpcClient.shutdown();
        }
    }
}
