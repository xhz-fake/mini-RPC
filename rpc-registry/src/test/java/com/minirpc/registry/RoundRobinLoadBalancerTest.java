package com.minirpc.registry;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoundRobinLoadBalancerTest {
    private static final String SERVICE_NAME = "com.minirpc.demo.service.HelloService";

    @Test
    public void shouldSelectInstancesInRoundRobinOrder() {// 轮询顺序对不对
        RoundRobinLoadBalancer loadBalancer = new RoundRobinLoadBalancer();
        ServiceInstance first = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9000);
        ServiceInstance second = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9001);
        ServiceInstance third = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9002);
        List<ServiceInstance> instances = List.of(first, second, third);

        //然后连续调四次：
        assertEquals(first, loadBalancer.select(instances, List.of()));
        assertEquals(second, loadBalancer.select(instances, List.of()));
        assertEquals(third, loadBalancer.select(instances, List.of()));
        assertEquals(first, loadBalancer.select(instances, List.of()));
        // 这个测试在证明：
        //- 第一次选第 1 台
        //- 第二次选第 2 台
        //- 第三次选第 3 台
        //- 第四次重新回到第 1 台
    }

    @Test
    public void shouldSkipExcludedInstancesDuringRoundRobinSelection() {// 排除已试实例是否生效
        RoundRobinLoadBalancer loadBalancer = new RoundRobinLoadBalancer();
        ServiceInstance first = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9000);
        ServiceInstance second = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9001);
        ServiceInstance third = new ServiceInstance(SERVICE_NAME, "127.0.0.1", 9002);
        List<ServiceInstance> instances = List.of(first, second, third);

        ServiceInstance selected = loadBalancer.select(instances, List.of(first, second));
        assertEquals(third, selected);
        // 这段在证明：
        //- 如果 A、B 已经被排除
        //- 候选里只剩 C
        //- 那就必须返回 C
    }

    @Test
    public void factoryShouldCreateExpectedLoadBalancer() {// 负载均衡工厂分发是否正确
        assertTrue(LoadBalancerFactory.create("random") instanceof RandomLoadBalancer);
        assertTrue(LoadBalancerFactory.create("round_robin") instanceof RoundRobinLoadBalancer);
        assertTrue(LoadBalancerFactory.create("rr") instanceof RoundRobinLoadBalancer);
        // 它在测的是：
        //- 工厂支持不同别名
        //- 并且能正确落到对应策略实现
    }
}
