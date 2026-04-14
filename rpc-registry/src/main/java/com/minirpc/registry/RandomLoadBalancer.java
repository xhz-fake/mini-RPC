package com.minirpc.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomLoadBalancer implements LoadBalancer {
    @Override
    public ServiceInstance select(List<ServiceInstance> instances, List<ServiceInstance> excludedInstances) {
        // 第一层保护：候选列表本身不能是空的。
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
        // 如果筛完一个都不剩，说明这次请求已经把所有实例都试过了。
        if (candidates.isEmpty()) {
            throw new IllegalStateException("没有剩余可重试的服务实例");
        }
        // Day4 先用最简单的随机策略：
        // 候选实例里随机挑一个，先把“能选出目标实例”这条主链路跑通。
        int index = ThreadLocalRandom.current().nextInt(candidates.size());// 随机生成一个合法下标
        return candidates.get(index);
        //这段代码本质上就是在做：
        //- 本次请求当前还没失败过的可用候选实例列表里有多个服务地址
        //- 随机生成一个下标
        //- 按下标取出其中一个实例
        //- 把这台实例返回给客户端
        //- 客户端接下来就连这台机器、把请求发给它
    }
}