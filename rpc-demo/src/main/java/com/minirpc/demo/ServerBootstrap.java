package com.minirpc.demo;

import com.minirpc.demo.service.HelloService;
import com.minirpc.demo.service.HelloServiceImpl;
import com.minirpc.registry.RegistryCenter;
import com.minirpc.registry.ServiceInstance;
import com.minirpc.registry.ZookeeperRegistryCenter;
import com.minirpc.server.RpcServer;
import com.minirpc.server.ServiceRegistry;

public class ServerBootstrap {
    public static void main(String[] args) {
        String host = System.getProperty("rpc.server.host", "127.0.0.1");
        int port = Integer.getInteger("rpc.server.port", 9000);// 1. 决定服务实例要监听的地址

        ServiceRegistry registry = new ServiceRegistry();
        registry.register(HelloService.class, new HelloServiceImpl());// 把业务实现注册到本地服务表

        // Day5：把“文件注册中心”升级成“ZooKeeper 注册中心”。
        // 变量类型仍然写 RegistryCenter，说明上层流程不依赖具体实现细节。
        RegistryCenter registryCenter = new ZookeeperRegistryCenter();// - 变量类型： RegistryCenter - 实际对象： ZookeeperRegistryCenter
        // 服务端连 ZooKeeper 是为了什么？
        //- 注册自己的实例节点
        //- 下线时注销
        //- 维持临时节点会话
        ServiceInstance instance = new ServiceInstance(HelloService.class.getName(), host, port);
        registryCenter.register(instance);// 告诉注册中心：“我这里有一个叫 HelloService 服务，地址在 127.0.0.1:9000”
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {// 它处理的是：JVM 整体要关机了，偏“JVM 退出级别”的善后
            registryCenter.unregister(instance);//主动执行会让实例节点 立刻删除 ，而不是等 ZooKeeper 会话超时后再删。
            registryCenter.close();// 关闭 Curator 客户端，断开和 ZooKeeper 的会话
        }));
        //当 JVM 准备退出时，
        //额外执行一段“收尾代码”：
        //1. 主动把这个实例从注册中心删掉
        //2. 关闭注册中心客户端连接

        //好处:
        // 1.客户端更早看不到这个实例
        // 2.表达主动注销，明确下线

        RpcServer rpcServer = new RpcServer(port, registry);
        System.out.println("server started at " + host + ":" + port);
        System.out.println("service registered: " + instance);
        try {
            rpcServer.start();
        } finally {// 它处理的是：当前这段 main 方法里的业务流程结束了,偏“代码块级别”的善后
            registryCenter.unregister(instance);
            registryCenter.close();
        }
    }
}