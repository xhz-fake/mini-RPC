package com.minirpc.registry;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.fail;

public class ZookeeperRegistryCenterTest {
    private static final String TEST_BASE_PATH = "/mini-rpc/test-services";
    private static final String SERVICE_NAME = "com.minirpc.demo.service.HelloService";

    private TestingServer testingServer;
    private ZookeeperRegistryCenter consumerRegistryCenter;
    private ZookeeperRegistryCenter providerRegistryCenter;

    @Before
    public void setUp() throws Exception {
        testingServer = new TestingServer(true);
        consumerRegistryCenter = new ZookeeperRegistryCenter(testingServer.getConnectString(), TEST_BASE_PATH);
        providerRegistryCenter = new ZookeeperRegistryCenter(testingServer.getConnectString(), TEST_BASE_PATH);
    }

    @After
    public void tearDown() throws Exception {
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
    public void shouldRefreshLocalCacheWhenInstancesChange() throws Exception {
        ServiceInstance firstInstance = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9000);
        ServiceInstance secondInstance = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9001);

        // 先让“服务提供方视角”的注册中心把第一个实例注册上去。
        providerRegistryCenter.register(firstInstance);
        // “服务消费者视角”的注册中心第一次 discover 后，应该能把它拉到本地缓存里。
        assertInstancesEventually(List.of(firstInstance));

        // discover() 返回的是副本。这里故意改掉返回值，确认不会污染注册中心内部缓存。
        List<ServiceInstance> snapshot = consumerRegistryCenter.discover(SERVICE_NAME);
        snapshot.clear();
        assertInstancesEventually(List.of(firstInstance));

        // 新实例上线后，不手动再次刷新缓存，依赖 watcher 自动感知并刷新。
        providerRegistryCenter.register(secondInstance);
        assertInstancesEventually(List.of(firstInstance, secondInstance));

        // 实例下线后，也依赖 watcher 自动把本地缓存同步回最新状态。
        providerRegistryCenter.unregister(secondInstance);
        assertInstancesEventually(List.of(firstInstance));
    }

    private void assertInstancesEventually(List<ServiceInstance> expectedInstances) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        Set<ServiceInstance> expectedSet = new HashSet<>(expectedInstances);
        while (System.currentTimeMillis() < deadline) {
            List<ServiceInstance> actualInstances = consumerRegistryCenter.discover(SERVICE_NAME);
            if (new HashSet<>(actualInstances).equals(expectedSet)) {
                return;
            }
            // watcher 刷新是异步的，所以这里轮询等一小会儿，让缓存有时间同步完成。
            Thread.sleep(100);
        }
        fail("等待本地缓存刷新超时，期望实例列表=" + expectedInstances
                + "，实际实例列表=" + consumerRegistryCenter.discover(SERVICE_NAME));
    }
}
