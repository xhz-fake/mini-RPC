package com.minirpc.server;

import com.minirpc.core.codec.RpcMessageCodec;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import com.minirpc.core.transport.RpcIO;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer {
    private final int port;
    private final ServiceRegistry serviceRegistry;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public RpcServer(int port, ServiceRegistry serviceRegistry) {
        this.port = port;
        this.serviceRegistry = serviceRegistry;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                // 接收一个客户端连接，并交给线程池处理，避免阻塞后续连接接入。
                Socket socket = serverSocket.accept();
                executorService.submit(() -> handle(socket));
            }
        } catch (IOException e) {
            throw new RuntimeException("服务端启动失败", e);
        }
    }

    private void handle(Socket socket) {
        try (socket) {
            // 1) 读取一条完整请求帧并反序列化为 RpcRequest。
            byte[] requestBytes = RpcIO.readFrame(socket.getInputStream());
            RpcRequest request = (RpcRequest) RpcMessageCodec.decode(requestBytes);

            System.out.println("requestId:"+ request.getRequestId());
            // 2) 调用本地服务实现，生成 RpcResponse。
            RpcResponse response = invoke(request);
            // 3) 把响应编码并回写给客户端。
            byte[] responseBytes = RpcMessageCodec.encode(response);
            RpcIO.writeFrame(socket.getOutputStream(), responseBytes);
        } catch (IOException e) {
            throw new RuntimeException("服务端处理请求失败", e);
        }
    }

    private RpcResponse invoke(RpcRequest request) {
        RpcResponse response = new RpcResponse();
        // requestId 回传，便于客户端和服务端做请求响应关联。
        response.setRequestId(request.getRequestId());
        try {
            // 根据接口名找到具体服务实现对象。
            Object service = serviceRegistry.getService(request.getInterfaceName());
            if (service == null) {
                throw new IllegalStateException("服务未注册: " + request.getInterfaceName());
            }
            // 用“方法名 + 参数类型”精确定位目标方法，支持方法重载场景。
            Method method = service.getClass().getMethod(request.getMethodName(), request.getParameterTypes());

            System.out.println("methodName:" + method.getName());
            // 反射执行真实业务方法。
            Object result = method.invoke(service, request.getArgs());//真正执行这个方法
            response.setData(result);// result 实际就是字符串。 result = 远程方法真正执行后的返回值，只是被统一装进 Object 里传输。
            System.out.println("result的类型:" + result.getClass());
            return response;
        } catch (Exception e) {
            // 出错时把错误信息写入响应，让客户端感知远端异常。
            response.setError(e.getMessage());
            return response;
        }
    }
}
