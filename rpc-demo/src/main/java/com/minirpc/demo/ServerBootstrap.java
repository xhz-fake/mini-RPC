package com.minirpc.demo;

import com.minirpc.demo.service.HelloService;
import com.minirpc.demo.service.HelloServiceImpl;
import com.minirpc.server.RpcServer;
import com.minirpc.server.ServiceRegistry;

public class ServerBootstrap {
    public static void main(String[] args) {
        ServiceRegistry registry = new ServiceRegistry();
        registry.register(HelloService.class, new HelloServiceImpl());

        RpcServer rpcServer = new RpcServer(9000, registry);
        System.out.println("server started at 127.0.0.1:9000");
        rpcServer.start();
    }
}
