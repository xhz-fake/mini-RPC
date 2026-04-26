package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;
import com.minirpc.registry.LoadBalancerFactory;
import com.minirpc.registry.RegistryCenter;
import com.minirpc.registry.ZookeeperRegistryCenter;

public class ClientBootstrap {// 把“注册中心底座”从文件，升级成了真正的中间件 ZooKeeper
    public static void main(String[] args) {
        // Day6：客户端继续按服务名发现实例，但 discover 背后已经升级为：
        // 1. 首次从 ZooKeeper 拉取实例列表
        // 2. 把结果缓存到当前 JVM
        // 3. 后续靠 watcher 自动刷新缓存
        RegistryCenter registryCenter = new ZookeeperRegistryCenter();
        // 客户端连 ZooKeeper 是为了什么？
        //- discover(serviceName)
        //- 查这个服务有哪些实例
        //- Day6 用来做 watcher / 本地缓存更新
        // Day7：默认可通过 -Drpc.loadbalancer=random / round_robin 切换负载均衡策略。
        RpcClient rpcClient = new RpcClient(registryCenter, LoadBalancerFactory.fromSystemProperty()); // 具体的负载均衡器的选择由 JVM 参数 rpc.loadbalancer 里取，比如 -Drpc.loadbalancer=rr
        try {
            RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
            HelloService helloService = rpcClientProxy.create(HelloService.class);
            String result1 = helloService.hello("mini-RPC-1");
            String result2 = helloService.hello("mini-RPC-2");
            System.out.println(result1);
            System.out.println(result2);
        } finally {
            rpcClient.shutdown();
            registryCenter.close();// 关闭客户端和 ZooKeeper 之间的 Curator 连接
        }
    }
}
