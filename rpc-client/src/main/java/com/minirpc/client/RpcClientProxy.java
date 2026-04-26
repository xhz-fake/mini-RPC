package com.minirpc.client;

import com.minirpc.client.exception.RemoteInvocationException;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;

public class RpcClientProxy {
    private final RpcClient rpcClient;

    public RpcClientProxy(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @SuppressWarnings("unchecked")
    public <T> T create(Class<T> interfaceClass) {// 返回的是 JDK 动态代理对象
        // 这里定义“代理对象被调用时”要执行的逻辑。
        // 你每次调用 helloService.xxx(...)，都会先进入这个 handler。
        InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {// 这三个参数不是你手动传的，是 JDK 动态代理在运行时自动传进来的。
            // handler 是“代理对象的方法一旦被调用，就要执行的逻辑”
            // 1) 代理层把“本地方法调用信息”转换成可传输的 RpcRequest。
            RpcRequest request = new RpcRequest();
            request.setRequestId(UUID.randomUUID().toString());
            request.setInterfaceName(interfaceClass.getName());
            request.setMethodName(method.getName());
            request.setParameterTypes(method.getParameterTypes());
            request.setArgs(args);
            // 这一步非常关键，因为它完成了：“方法调用” -> “可传输请求对象
            System.out.println("requestId:" + request.getRequestId());
            System.out.println("methodName:" + method.getName());

            // 2) 发起远程请求并阻塞等待响应。
            RpcResponse response = rpcClient.sendRequest(request);// 1.RpcClient.sendRequest() 负责的是通信层拿回 RpcResponse
            // 3) 把服务端错误转换为本地异常，保证调用方能感知失败。
            if (response.hasError()) {// 2.但“这个响应是不是业务失败”这件事，是代理层决定的
                //- 服务端哪怕业务报错，只要它能正常把错误封装进 RpcResponse
                //- 从通信层角度，这次请求其实是“成功收到了响应”
                //- 只是这个响应里带的是业务错误而不是业务数据
                throw new RemoteInvocationException(response.getError());//所以这里不能把它当成 RpcConnectionException 或 RpcTimeoutException必须单独区分成 RemoteInvocationException
            }
            // 4) 成功时返回响应数据，行为上等价于本地方法返回值。
            return response.getData();
        };

        // 创建并返回 JDK 动态代理对象。
        return (T) Proxy.newProxyInstance(// 我们项目里的代理对象来自本行
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                handler
        );// 意思是：让 JDK 在运行时生成一个对象; 这个对象实现 HelloService 接口 ; 这个对象的所有方法调用，都交给 handler 处理
//
//        你项目里的代理对象到底替你做了什么
//        在你这个 mini-RPC 里，这个代理对象的职责非常明确：
//        调用方写的是： helloService.hello("mini-RPC-1");
//        但代理对象实际做的是：
//
//        -拦截这次方法调用
//        -拿到接口名、方法名、参数类型、参数值
//        -组装成 RpcRequest
//        -调 rpcClient.sendRequest(request)
//        -拿到 RpcResponse
//        -返回 response.getData()
//        -也就是说，在你的项目里，代理对象做的事情是： 把“本地方法调用”伪装成“远程服务调用”。
//
//        这是 RPC 客户端里代理最经典的用法。
    }
}
