package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.client.exception.RemoteInvocationException;
import com.minirpc.client.exception.RpcConnectionException;
import com.minirpc.client.exception.RpcTimeoutException;
import com.minirpc.client.exception.ServiceDiscoveryException;
import com.minirpc.registry.RoundRobinLoadBalancer;
import com.minirpc.registry.ServiceInstance;
import com.minirpc.registry.ZookeeperRegistryCenter;
import com.minirpc.server.RpcServer;
import com.minirpc.server.ServiceRegistry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class Day7RpcIntegrationTest {
    //同时证明 4 条 Day7 主线：
    //- 序列化策略切换真的生效
    //- 轮询负载均衡真的生效
    //- 异常分类真的生效
    //- 这些能力不是孤立存在，而是已经被串到完整 RPC 调用链里

    private static final String TEST_BASE_PATH = "/mini-rpc/day7-tests";

    private final List<ZookeeperRegistryCenter> providerRegistryCenters = new ArrayList<>();
    private final List<ZookeeperRegistryCenter> consumerRegistryCenters = new ArrayList<>();
    private final List<RpcClient> rpcClients = new ArrayList<>();

    private TestingServer testingServer;

    @Before
    public void setUp() throws Exception {
        testingServer = new TestingServer(true);
    }
    // 它不是让你去手动启动本机 ZooKeeper，
    // 而是在测试里自己拉起一个 内嵌 ZooKeeper 。

    @After
    public void tearDown() throws Exception {
        for (RpcClient rpcClient : rpcClients) {
            rpcClient.shutdown();
        }
        for (ZookeeperRegistryCenter registryCenter : consumerRegistryCenters) {
            registryCenter.close();
        }
        for (ZookeeperRegistryCenter registryCenter : providerRegistryCenters) {
            registryCenter.close();
        }
        if (testingServer != null) {
            testingServer.close();
        }
        System.clearProperty("rpc.serializer");
        System.clearProperty("rpc.client.timeout.seconds");
        System.clearProperty("rpc.client.retry.maxAttempts");
        System.clearProperty("rpc.loadbalancer");
        // 所以这里实际上是在保证：
        //- 每个测试用例的环境干净
        //- 不会互相串味
    }

    @Test
    public void shouldCompleteRpcCallWithJsonSerializer() throws Exception {
        System.setProperty("rpc.serializer", "json");

        startServerAndRegister(EchoService.class, new EchoServiceImpl("json"), findFreePort());
        EchoService echoService = createProxy(EchoService.class, new RoundRobinLoadBalancer());

        assertEquals("hello json-mini-rpc", echoService.hello("mini-rpc"));
    }

    @Test
    public void shouldDistributeRequestsWithRoundRobinLoadBalancer() throws Exception {// 轮询负载均衡是否真的生效
        startServerAndRegister(IdentityService.class, new IdentityServiceImpl("server-A"), findFreePort());
        startServerAndRegister(IdentityService.class, new IdentityServiceImpl("server-B"), findFreePort());

        IdentityService identityService = createProxy(IdentityService.class, new RoundRobinLoadBalancer());
        String first = identityService.identity();
        String second = identityService.identity();
        String third = identityService.identity();
        String fourth = identityService.identity();

        assertNotEquals(first, second);
        assertEquals(first, third);
        assertEquals(second, fourth);
    }
    //也就是说：
    //- 轮询逻辑不是只在单元测试里成立
    //- 它真的已经被接进 RpcClient -> 服务发现 -> 实例选择 -> 真实调用 这条链里了

    @Test
    public void shouldThrowServiceDiscoveryExceptionWhenNoProviderExists() {// 没有 provider 时是否抛对异常
        try {
            MissingService missingService = createProxy(MissingService.class, new RoundRobinLoadBalancer());
            missingService.missing();
            fail("预期应抛出 ServiceDiscoveryException");
        } catch (ServiceDiscoveryException expected) {
            assertEquals("没有发现可用服务实例: " + MissingService.class.getName(), expected.getMessage());
        }
    }
    // 这个测试在证明：
    //- Day7 异常分类里，“发现不到实例”这件事确实被归成了服务发现异常
    //- 不是笼统地抛个 RuntimeException

    @Test
    public void shouldThrowRpcConnectionExceptionWhenServerIsUnavailable() throws IOException {// 服务端不可达时是否抛连接异常
        int unusedPort = findFreePort();// 先找一个空闲端口
        RpcClient rpcClient = new RpcClient("127.0.0.1", unusedPort);
        rpcClients.add(rpcClient);
        // 但故意不启动服务端

        DirectService directService = new RpcClientProxy(rpcClient).create(DirectService.class);// 然后客户端直连这个端口发请求
        try {
            directService.direct();
            fail("预期应抛出 RpcConnectionException");
        } catch (RpcConnectionException expected) {
            assertEquals(true, expected.getMessage().contains("连接服务端失败"));
        }
    }
    // 这个测试非常干净地证明了：
    //- “连不上服务端”这个场景被清楚分类成连接异常

    //而且这里顺带也说明了：
    //- 直连模式构造器不是摆设
    //- 它在测试里很有用

    @Test
    public void shouldThrowRpcTimeoutExceptionWhenResponseIsTooSlow() throws Exception {// 超时是否抛对异常
        System.setProperty("rpc.client.timeout.seconds", "1");
        System.setProperty("rpc.client.retry.maxAttempts", "1");
        // 意思是：
        //- 超时阈值改成 1 秒
        //- 重试次数设成 1，避免重试干扰这个测试

        startServerAndRegister(SlowService.class, new SlowServiceImpl(), findFreePort());
        SlowService slowService = createProxy(SlowService.class, new RoundRobinLoadBalancer());
        try {
            slowService.slow("timeout");// 然后起一个慢服务
            fail("预期应抛出 RpcTimeoutException");
            // 这就稳定制造出：
            //- 服务端确实会响应
            //- 但来得太晚
        } catch (RpcTimeoutException expected) {
            assertEquals(true, expected.getMessage().contains("远程调用超时"));
        }
    }
    //这个测试在证明：
    //- 不是连接失败
    //- 不是服务发现失败
    //- 而是等待响应超时

    @Test
    public void shouldThrowRemoteInvocationExceptionWhenServerBusinessFails() throws Exception {// 远端业务异常是否抛对类型
        startServerAndRegister(FailingService.class, new FailingServiceImpl(), findFreePort());
        FailingService failingService = createProxy(FailingService.class, new RoundRobinLoadBalancer());
        try {
            failingService.fail("boom");
            fail("预期应抛出 RemoteInvocationException");
        } catch (RemoteInvocationException expected) {
            assertEquals("模拟远端业务异常: boom", expected.getMessage());
        }
    }
    // 这个测试证明：
    //- 请求其实已经到服务端并执行到业务逻辑了
    //- 只是业务方法自己报错
    //- 所以客户端应该感知为“远端业务异常”，而不是“网络失败”

    private <T> T createProxy(Class<T> serviceInterface, RoundRobinLoadBalancer loadBalancer) {
        ZookeeperRegistryCenter consumerRegistryCenter =
                new ZookeeperRegistryCenter(testingServer.getConnectString(), TEST_BASE_PATH);
        consumerRegistryCenters.add(consumerRegistryCenter);
        RpcClient rpcClient = new RpcClient(consumerRegistryCenter, loadBalancer);
        rpcClients.add(rpcClient);
        return new RpcClientProxy(rpcClient).create(serviceInterface);
    }

    private void startServerAndRegister(Class<?> serviceInterface, Object serviceImpl, int port) throws Exception {
        ServiceRegistry serviceRegistry = new ServiceRegistry();
        serviceRegistry.register(serviceInterface, serviceImpl);

        RpcServer rpcServer = new RpcServer(port, serviceRegistry);
        Thread serverThread = new Thread(rpcServer::start, "day7-rpc-test-" + port);
        serverThread.setDaemon(true);
        serverThread.start();
        waitUntilPortReady(port);

        ZookeeperRegistryCenter providerRegistryCenter =
                new ZookeeperRegistryCenter(testingServer.getConnectString(), TEST_BASE_PATH);
        providerRegistryCenter.register(new ServiceInstance(serviceInterface.getName(), "127.0.0.1", port));
        providerRegistryCenters.add(providerRegistryCenter);
    }

    private int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private void waitUntilPortReady(int port) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return;
            } catch (IOException ignored) {
                Thread.sleep(100);
            }
        }
        fail("等待服务端端口就绪超时: " + port);
    }

    public interface EchoService {
        String hello(String name);
    }

    public static class EchoServiceImpl implements EchoService {
        private final String prefix;

        public EchoServiceImpl(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String hello(String name) {
            return "hello " + prefix + "-" + name;
        }
    }

    public interface IdentityService {
        String identity();
    }

    public static class IdentityServiceImpl implements IdentityService {
        private final String identity;

        public IdentityServiceImpl(String identity) {
            this.identity = identity;
        }

        @Override
        public String identity() {
            return identity;
        }
    }

    public interface MissingService {
        String missing();
    }

    public interface DirectService {
        String direct();
    }

    public interface SlowService {
        String slow(String name);
    }

    public static class SlowServiceImpl implements SlowService {
        @Override
        public String slow(String name) {// 然后起一个慢服务：
            try {
                Thread.sleep(1_500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("slow interrupted", e);
            }
            return "hello " + name;
        }
    }

    public interface FailingService {
        String fail(String name);
    }

    public static class FailingServiceImpl implements FailingService {
        @Override
        public String fail(String name) {
            throw new IllegalStateException("模拟远端业务异常: " + name);
        }
    }
}

//这些测试按“层次”记忆 最清楚的记法是：
//- SerializerTest ：测序列化组件正确性
//- RoundRobinLoadBalancerTest ：测负载均衡策略正确性
//- Day7RpcIntegrationTest ：测 RPC 整体行为正确性

//这个分层很像真实工程里常见的测试思路：
//- 1.底层组件单测
//- 2.策略单测
//- 3.端到端集成验证

//- 它覆盖了 Day7 的 4 个关键场景：
//- shouldCompleteRpcCallWithJsonSerializer() ：证明 JSON 序列化已经真正接进完整 RPC 主链
//- shouldDistributeRequestsWithRoundRobinLoadBalancer() ：证明轮询负载均衡不是只在单测里对，而是真的能控制真实请求分发
//- shouldThrowServiceDiscoveryExceptionWhenNoProviderExists() 、 shouldThrowRpcConnectionExceptionWhenServerIsUnavailable() 、 shouldThrowRpcTimeoutExceptionWhenResponseIsTooSlow() 、 shouldThrowRemoteInvocationExceptionWhenServerBusinessFails() ：证明异常分类在不同失败场景下真的能抛出正确类型
//- 这组集成测试的意义非常大，因为它在替你回答：
//- “这些 Day7 新能力不是分散的类，而是已经被串进完整 RPC 调用链里了”