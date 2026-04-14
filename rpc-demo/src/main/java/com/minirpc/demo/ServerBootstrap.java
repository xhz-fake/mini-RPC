package com.minirpc.demo;

import com.minirpc.demo.service.HelloService;
import com.minirpc.demo.service.HelloServiceImpl;
import com.minirpc.registry.FileRegistryCenter;
import com.minirpc.registry.RegistryCenter;
import com.minirpc.registry.ServiceInstance;
import com.minirpc.server.RpcServer;
import com.minirpc.server.ServiceRegistry;

public class ServerBootstrap {
    public static void main(String[] args) {
        String host = System.getProperty("rpc.server.host", "127.0.0.1");
        int port = Integer.getInteger("rpc.server.port", 9000);

        ServiceRegistry registry = new ServiceRegistry();
        registry.register(HelloService.class, new HelloServiceImpl());

        RegistryCenter registryCenter = new FileRegistryCenter();// - 变量类型： RegistryCenter - 实际对象： FileRegistryCenter
        ServiceInstance instance = new ServiceInstance(HelloService.class.getName(), host, port);
        registryCenter.register(instance);// 告诉注册中心：“我这里有一个叫 HelloService 服务，地址在 127.0.0.1:9000”
        Runtime.getRuntime().addShutdownHook(new Thread(() -> registryCenter.unregister(instance)));

        RpcServer rpcServer = new RpcServer(port, registry);
        System.out.println("server started at " + host + ":" + port);
        System.out.println("service registered: " + instance);
        try {
            rpcServer.start();
        } finally {
            registryCenter.unregister(instance);
        }
    }
}