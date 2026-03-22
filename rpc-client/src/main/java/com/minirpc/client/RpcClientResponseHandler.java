package com.minirpc.client;

import com.minirpc.core.protocol.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.CountDownLatch;

public class RpcClientResponseHandler extends SimpleChannelInboundHandler<RpcResponse> {
    // 倒计时门闩：1 表示“还没收到响应”，0 表示“响应已到达”。
    // 这样可以把 Netty 的异步回调模型，桥接成上层可理解的同步等待模型。
    private final CountDownLatch latch = new CountDownLatch(1);
    // 保存服务端返回结果，awaitResponse() 被唤醒后会读取这个字段。
    private RpcResponse response;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) {// 客户端受到响应
        // Netty 收到响应后会回调这里。
        this.response = msg;
        // 告诉等待线程“响应到了，可以继续往下走”。
        latch.countDown();
    }

    public RpcResponse awaitResponse() throws InterruptedException {
        // 调用线程会阻塞在这里，直到 channelRead0 收到响应并 countDown。
        latch.await();
        return response;
    }
}
