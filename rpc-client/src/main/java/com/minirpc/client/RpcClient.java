package com.minirpc.client;

import com.minirpc.core.codec.RpcMessageCodec;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import com.minirpc.core.transport.RpcIO;

import java.io.IOException;
import java.net.Socket;

public class RpcClient {
    private final String host;
    private final int port;

    public RpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RpcResponse sendRequest(RpcRequest request) {
        // Day1 的实现策略：一次调用创建一次短连接，先保证链路清晰可验证。
        try (Socket socket = new Socket(host, port)) {
            // 1) 请求对象编码为字节数组。
            byte[] requestBytes = RpcMessageCodec.encode(request);
            // 2) 按“长度 + 内容”发送，避免接收端无法识别消息边界。
            RpcIO.writeFrame(socket.getOutputStream(), requestBytes);

            // 3) 读取服务端响应帧并反序列化为 RpcResponse。
            byte[] responseBytes = RpcIO.readFrame(socket.getInputStream());
            return (RpcResponse) RpcMessageCodec.decode(responseBytes);
        } catch (IOException e) {
            throw new RuntimeException("远程调用失败", e);
        }
    }
}
