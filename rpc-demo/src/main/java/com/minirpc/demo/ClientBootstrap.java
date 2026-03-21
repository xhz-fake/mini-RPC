package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;

public class ClientBootstrap {
    public static void main(String[] args) {
        // 1) 配置远程服务地址。当前 Day1 使用固定地址直连，后续才会替换成注册中心发现。
        RpcClient rpcClient = new RpcClient("127.0.0.1", 9000);
        // 2) 创建代理工厂。它负责把“本地方法调用”转换为“远程请求”。
        RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
        // 3) 生成 HelloService 代理对象。此时拿到的不是实现类，而是 JDK 动态代理对象。
        HelloService helloService = rpcClientProxy.create(HelloService.class);
        // 4) 看似本地调用，实际会进入 InvocationHandler，组装 RpcRequest 并发给服务端。
        String result = helloService.hello("mini-RPC");
        // 5) 服务端执行后返回结果，代理层把 RpcResponse.data 还原成方法返回值。
        System.out.println("rpc result: " + result);
    }
}
