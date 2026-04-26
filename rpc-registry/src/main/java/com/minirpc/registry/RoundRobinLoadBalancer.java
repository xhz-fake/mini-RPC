package com.minirpc.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinLoadBalancer implements LoadBalancer {
    // 轮询的核心就是维护一个不断递增的游标。
    // 每次选实例时，把游标对候选数取模，就能按顺序轮流落到不同实例上。
    private final AtomicInteger sequence = new AtomicInteger();// 这个 sequence 你可以把它理解成：一个不断递增的“轮询指针”
    // 因为它是线程安全的递增计数器。你这个项目未来在并发请求下，多个线程都可能来选实例。 如果这里只是普通 int ，就可能出现并发下计数错乱。
    @Override
    public ServiceInstance select(List<ServiceInstance> instances, List<ServiceInstance> excludedInstances) {
        if (instances == null || instances.isEmpty()) {
            throw new IllegalArgumentException("没有可用服务实例");
        }
        // 先把“本次请求还没试过的实例”筛出来。
        List<ServiceInstance> candidates = new ArrayList<>();
        for (ServiceInstance instance : instances) {
            if (excludedInstances == null || !excludedInstances.contains(instance)) {
                candidates.add(instance);
            }
        }
        if (candidates.isEmpty()) {
            throw new IllegalStateException("没有剩余可重试的服务实例");
        }
        // getAndIncrement() 的含义是：
        // 1. 先把当前 sequence 的值拿出来参与本次选择
        // 2. 再把 sequence 自增 1
        //
        // 假设 candidates.size() = 3，sequence 初始是 0：
        // - 第一次：0 % 3 = 0，选第 0 个
        // - 第二次：1 % 3 = 1，选第 1 个
        // - 第三次：2 % 3 = 2，选第 2 个
        // - 第四次：3 % 3 = 0，又回到第 0 个
        //
        // 这就是“轮询”最本质的实现方式：不断递增一个游标，再对候选数取模。
        // Math.floorMod 可以避免 sequence 溢出后出现负下标。
        int index = Math.floorMod(sequence.getAndIncrement(), candidates.size());
        return candidates.get(index);
    }
}
