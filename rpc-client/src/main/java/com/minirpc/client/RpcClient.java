package com.minirpc.client;

import com.minirpc.core.codec.netty.RpcMessageDecoder;
import com.minirpc.core.codec.netty.RpcMessageEncoder;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class RpcClient {
    private final String host;
    private final int port;

    public RpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RpcResponse sendRequest(RpcRequest request) {
        // EventLoopGroup 可以理解成 Netty 的“线程调度器”。
        // 这里用 1 个线程是为了先把链路讲清楚，后续可以按并发量调优。
        EventLoopGroup group = new NioEventLoopGroup(1);// 创建客户端事件循环组
        try {
            // 这个处理器负责接收服务端返回的 RpcResponse。
            // sendRequest 里会阻塞等待它拿到响应，保持“同步调用体验”。
            RpcClientResponseHandler responseHandler = new RpcClientResponseHandler();

            // Bootstrap 是 Netty 客户端启动器，负责配置连接参数和 pipeline。
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    // 指定客户端通道类型：NIO Socket 通道。
                    .channel(NioSocketChannel.class)
                    // 关闭 Nagle 算法，减少小包场景的发送延迟。
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 入站（服务端回包）：先拆帧，再转对象
                            // 入站第 1 步：按长度字段拆包，拿到完整的一帧消息。
                            // 参数含义：最大帧长、长度字段偏移、长度字段字节数、长度修正、跳过字节数。
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                            // 入站第 2 步：把字节还原成 Java 对象（RpcResponse）。
                            pipeline.addLast(new RpcMessageDecoder());

                            // 出站（客户端发包）：先转字节，再加长度头
                            // 出站第 1 步：在消息体前面写 4 字节长度字段。
                            pipeline.addLast(new LengthFieldPrepender(4));
                            // 出站第 2 步：把 Java 对象编码成字节数组。
                            pipeline.addLast(new RpcMessageEncoder());

                            // 入站第 3 步：业务处理器，真正接收响应并唤醒等待线程。
                            pipeline.addLast(responseHandler);
                        }
                    });

            // 发起 TCP 连接并等待连接建立成功。
            Channel channel = bootstrap.connect(host, port).sync().channel(); // 客户端发送请求前
            // 发送 RpcRequest。writeAndFlush 会触发出站编码流程。
            channel.writeAndFlush(request).sync();// -----发送 RpcRequest，发请求是出站
            // 阻塞等待服务端响应，直到 responseHandler 收到回包并 countDown。
            RpcResponse response = responseHandler.awaitResponse();
            // 当前版本是一请求一连接，用完即关；后续会优化为连接复用。
            channel.close().sync();
            return response;
        } catch (InterruptedException e) {
            // 保留中断语义，避免吞掉中断信号。
            Thread.currentThread().interrupt();
            throw new RuntimeException("远程调用失败", e);
        } finally {
            // 优雅关闭线程组，释放 Netty 资源。
            group.shutdownGracefully();
        }
    }
}
