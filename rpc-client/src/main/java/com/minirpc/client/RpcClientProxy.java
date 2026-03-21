package com.minirpc.client;

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
    public <T> T create(Class<T> interfaceClass) {
        // 这里定义“代理对象被调用时”要执行的逻辑。
        // 你每次调用 helloService.xxx(...)，都会先进入这个 handler。
        InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {
            // 1) 把本地方法调用信息封装成可传输的 RpcRequest。
            RpcRequest request = new RpcRequest();
            request.setRequestId(UUID.randomUUID().toString());
            request.setInterfaceName(interfaceClass.getName());
            request.setMethodName(method.getName());
            request.setParameterTypes(method.getParameterTypes());
            request.setArgs(args);

            // 2) 发起远程请求并阻塞等待响应。
            RpcResponse response = rpcClient.sendRequest(request);
            // 3) 把服务端错误转换为本地异常，保证调用方能感知失败。
            if (response.hasError()) {
                throw new RuntimeException(response.getError());
            }
            // 4) 成功时返回响应数据，行为上等价于本地方法返回值。
            return response.getData();
        };

        // 创建并返回 JDK 动态代理对象。
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                handler
        );
    }
}
