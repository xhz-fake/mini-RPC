package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;
import com.minirpc.registry.FileRegistryCenter;
import com.minirpc.registry.RandomLoadBalancer;

public class ClientBootstrap {
    public static void main(String[] args) {
        // Day4：客户端不再直连固定 IP，而是先通过注册中心发现服务实例。
        RpcClient rpcClient = new RpcClient(new FileRegistryCenter(), new RandomLoadBalancer()); // - 注册中心：负责查服务在哪 ; 负载均衡器：负责多个实例里挑一个
        try {
            RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
            HelloService helloService = rpcClientProxy.create(HelloService.class);
            String result1 = helloService.hello("mini-RPC-1");
            String result2 = helloService.hello("mini-RPC-2");
            System.out.println(result1);
            System.out.println(result2);
        } finally {
            rpcClient.shutdown();
        }
    }
}
