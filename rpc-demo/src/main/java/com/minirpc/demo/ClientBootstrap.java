package com.minirpc.demo;

import com.minirpc.client.RpcClient;
import com.minirpc.client.RpcClientProxy;
import com.minirpc.demo.service.HelloService;

public class ClientBootstrap { // Day3 仍然只改通信治理
    public static void main(String[] args) {
        // 创建客户端通信组件：Day3 版本内部支持连接复用、请求超时和响应映射。
        RpcClient rpcClient = new RpcClient("127.0.0.1", 9000);
        try {
            // 代理层保持不变：业务代码依然通过接口方法发起调用。
            RpcClientProxy rpcClientProxy = new RpcClientProxy(rpcClient);
            HelloService helloService = rpcClientProxy.create(HelloService.class);// 代理对象：helloService
            // 这里的 helloService 表面类型是 HelloService，但运行时真实对象其实是类似：$Proxy0 ; jdk.proxy1.$Proxy0
            // 业务调用语义不变，通信治理能力由 RpcClient 内部增强。
//            String result = helloService.hello("mini-RPC");
//            System.out.println("rpc result: " + result);
            String result1 = helloService.hello("mini-RPC-1"); // 表面上是调用 hello()，实际上是调用到了 代理类生成的 hello() 方法。
            String result2 = helloService.hello("mini-RPC-2"); // 实际上是“把远程调用包装成本地调用的体验。”
            System.out.println(result1);
            System.out.println(result2);
        } finally {
            // 程序退出前主动释放连接与 Netty 线程资源。
            rpcClient.shutdown();
        }
    }
}
