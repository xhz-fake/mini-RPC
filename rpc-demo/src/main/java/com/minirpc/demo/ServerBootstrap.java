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

//完整的数据传输流程
/*
    你可以把全链路背成这条线：

    1.客户端 main 线程调用 helloService.hello("mini-RPC-1")
    2.进入 RpcClientProxy 的动态代理
    3.组装 RpcRequest
    4.RpcClient.sendRequest() 创建 future，并 pendingRequests.put(requestId, future)
    5.ensureConnected() 复用或创建 channel
    6.writeAndFlush(request) 把 RpcRequest 编码成字节发出去
    7.服务端 worker 线程收到字节，解码成 RpcRequest
    8.RpcServerRequestHandler 调 rpcServer.invoke(request)
    9.ServiceRegistry 按接口名找实现对象
    10.反射执行目标方法，拿到结果
    11.组装 RpcResponse，带回同一个 requestId
    12.服务端把 RpcResponse 编码发回客户端
    13.客户端 I/O 线程解码出 RpcResponse
    14.RpcClientResponseHandler 用 requestId 从 pendingRequests 里 remove 对应 future
    15.future.complete(response) 唤醒原来等待的调用线程
    16.sendRequest() 返回 RpcResponse
    17.代理层返回 response.getData()
    18.ClientBootstrap 打印结果

    这 18 步就是你整个 Day3 的核心闭环。
*/