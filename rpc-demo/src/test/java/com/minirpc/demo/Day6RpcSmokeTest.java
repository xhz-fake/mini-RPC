package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;
import com.minirpc.demo.service.HelloServiceImpl;
import com.minirpc.registry.RandomLoadBalancer;
import com.minirpc.registry.ServiceInstance;
import com.minirpc.registry.ZookeeperRegistryCenter;
import com.minirpc.server.RpcServer;
import com.minirpc.server.ServiceRegistry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.Assert.assertEquals;

public class Day6RpcSmokeTest {
    private static final String TEST_BASE_PATH = "/mini-rpc/day6-smoke";

    private TestingServer testingServer;
    private ZookeeperRegistryCenter providerRegistryCenter;
    private ZookeeperRegistryCenter consumerRegistryCenter;
    private RpcClient rpcClient;

    @Before
    public void setUp() throws Exception {
        testingServer = new TestingServer(true);
    }

    @After
    public void tearDown() throws Exception {
        if (rpcClient != null) {
            rpcClient.shutdown();
        }
        if (consumerRegistryCenter != null) {
            consumerRegistryCenter.close();
        }
        if (providerRegistryCenter != null) {
            providerRegistryCenter.close();
        }
        if (testingServer != null) {
            testingServer.close();
        }
    }

    @Test
    public void shouldCompleteHelloCallWithLocalCacheDiscovery() throws Exception {
        int port = findFreePort();

        // 先准备服务端本地服务表：接口 -> 真实实现类。
        ServiceRegistry serviceRegistry = new ServiceRegistry();
        serviceRegistry.register(HelloService.class, new HelloServiceImpl());

        RpcServer rpcServer = new RpcServer(port, serviceRegistry);
        Thread serverThread = new Thread(rpcServer::start, "day6-rpc-smoke-server");
        // 测试结束时不需要手动 join 这个线程，让 JVM 退出时直接结束即可。
        serverThread.setDaemon(true);
        serverThread.start();

        // 给 Netty 服务端一点启动时间，确保端口已经开始监听。
        Thread.sleep(500);

        // providerRegistryCenter 站在“服务端注册自己”的视角。
        providerRegistryCenter = new ZookeeperRegistryCenter(testingServer.getConnectString(), TEST_BASE_PATH);
        ServiceInstance instance = new ServiceInstance(HelloService.class.getName(), "127.0.0.1", port);
        providerRegistryCenter.register(instance);

        // consumerRegistryCenter 站在“客户端发现服务”的视角。
        consumerRegistryCenter = new ZookeeperRegistryCenter(testingServer.getConnectString(), TEST_BASE_PATH);
        rpcClient = new RpcClient(consumerRegistryCenter, new RandomLoadBalancer());
        RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
        HelloService helloService = rpcClientProxy.create(HelloService.class);

        // 这一步会真实走完整条链：
        // 动态代理 -> RpcClient -> discover(缓存优先) -> Netty 发送 -> 服务端执行 -> 响应返回
        String result = helloService.hello("day6-smoke");
        assertEquals("hello day6-smoke", result);
    }

    private int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
