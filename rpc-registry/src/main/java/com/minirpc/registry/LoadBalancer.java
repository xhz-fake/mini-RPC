package com.minirpc.registry;

import java.util.List;

public interface LoadBalancer {
    // 从多个候选实例里选一个“这次请求真正要打过去的目标实例”。
    // excludedInstances 表示“本次请求已经试过但失败的实例”，负载均衡器应尽量避开它们。
    ServiceInstance select(List<ServiceInstance> instances, List<ServiceInstance> excludedInstances);
}
