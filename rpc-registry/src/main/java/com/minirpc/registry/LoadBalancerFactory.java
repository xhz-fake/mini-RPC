package com.minirpc.registry;

import java.util.Locale;

public final class LoadBalancerFactory {// 更接近“静态工厂方法”它自己不关心 random 还是 round_robin 的细节，它只是先去拿配置，然后把活转交给 create(...)
    private static final String LOAD_BALANCER_PROPERTY = "rpc.loadbalancer";
    private static final String DEFAULT_LOAD_BALANCER = "random";

    private LoadBalancerFactory() {// 禁止实例化
    }

    public static LoadBalancer fromSystemProperty() {
        return create(System.getProperty(LOAD_BALANCER_PROPERTY, DEFAULT_LOAD_BALANCER));
    }

    public static LoadBalancer create(String strategy) {
        String normalizedStrategy = strategy == null
                ? DEFAULT_LOAD_BALANCER
                : strategy.trim().toLowerCase(Locale.ROOT);
        return switch (normalizedStrategy) {
            case "", "random" -> new RandomLoadBalancer();
            case "round_robin", "roundrobin", "round-robin", "rr" -> new RoundRobinLoadBalancer();
            default -> throw new IllegalArgumentException("不支持的负载均衡策略: " + strategy);
        };
    }
}
//- 这个类本身没有“对象状态”要保存: 因此不需要对象

//什么叫“没有对象状态”?
//- 它没有成员变量需要在构造时初始化
//- 它不需要记住“上一次创建了什么”
//- 它不需要维护连接、缓存、计数器
//- 它只是“根据入参或系统配置，返回一个负载均衡器对象”
//这类类本质上更像“工具类 + 创建入口”，所以写成静态方法最自然。


//同时防止人误以为：
//- 这个工厂对象内部是不是有配置状态
//- 是不是不同工厂实例会创建出不同结果
//- 是不是要被 Spring 管理成 Bean