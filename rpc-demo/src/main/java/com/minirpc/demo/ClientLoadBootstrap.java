package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;
import com.minirpc.registry.LoadBalancerFactory;
import com.minirpc.registry.RegistryCenter;
import com.minirpc.registry.ZookeeperRegistryCenter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientLoadBootstrap {// 作用：跑并发压测/负载测试
    public static void main(String[] args) throws InterruptedException {
        int threadCount = Integer.getInteger("rpc.load.threadCount", 8);
        int requestsPerThread = Integer.getInteger("rpc.load.requestsPerThread", 20);
        // Day6：并发压测入口继续复用同一个 RpcClient，
        // 但实例列表已经优先从当前 JVM 本地缓存里拿；
        // ZooKeeper 的 watcher 会在后台持续帮我们刷新这份缓存。
        RegistryCenter registryCenter = new ZookeeperRegistryCenter();
        // Day7：压测入口也支持通过 JVM 参数切换负载均衡策略，便于做随机/轮询行为验证。
        RpcClient rpcClient = new RpcClient(registryCenter, LoadBalancerFactory.fromSystemProperty());
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            System.out.println("load config threadCount=" + threadCount
                    + ", requestsPerThread=" + requestsPerThread
                    + ", loadBalancer=" + System.getProperty("rpc.loadbalancer", "random")
                    + ", serializer=" + System.getProperty("rpc.serializer", "jdk"));
            RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
            HelloService helloService = rpcClientProxy.create(HelloService.class);

            CountDownLatch latch = new CountDownLatch(threadCount);
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
            long startNanos = System.nanoTime();
            for (int t = 0; t < threadCount; t++) {
                int threadIndex = t;
                executorService.submit(() -> {
                    try {
                        for (int i = 0; i < requestsPerThread; i++) {
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
            executorService.shutdownNow();
            rpcClient.shutdown();
            registryCenter.close();
        }
    }
}
