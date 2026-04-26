package com.minirpc.server;

import com.minirpc.core.codec.netty.RpcMessageDecoder;
import com.minirpc.core.codec.netty.RpcMessageEncoder;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
// RpcServer 的核心职责是监听端口、接收请求、根据接口名和方法签名定位服务实现，通过反射执行业务方法，再把执行结果封装成响应返回给客户端。
public class RpcServer {// 站在服务提供方视角，接收请求、找到目标服务、执行方法、返回结果，关心两件事：1.我监听哪个端口 2.我能提供哪些服务
    private final int port;
    private final ServiceRegistry serviceRegistry;

    public RpcServer(int port, ServiceRegistry serviceRegistry) {
        this.port = port;
        this.serviceRegistry = serviceRegistry;
    }

    public void start() {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);// bossGroup 只负责“接入连接”，不做耗时业务处理。

        EventLoopGroup workerGroup = new NioEventLoopGroup(); // workerGroup 负责读写 IO 与执行 ChannelHandler。
        try {
            // ServerBootstrap 是 Netty 服务端启动器。
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    // 服务端通道类型：NIO ServerSocket 通道。
                    .channel(NioServerSocketChannel.class)
                    // 半连接队列长度。
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    // 降低小包延迟。
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();// 服务端最先收到的是：TCP 字节流，也就是：[4字节长度][请求体字节]
                            // 入站：先按长度拆出完整帧，防止粘包/半包。把 TCP 字节流切成完整帧
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));// 这是服务端把“字节流切成完整一帧”的地方。
                            // 入站：把字节帧转成 RpcRequest/RpcResponse 对象，把完整帧字节反序列化成 Java 对象
                            pipeline.addLast(new RpcMessageDecoder());// 这里就是服务端把网络字节真正“还原成 Java 对象”的地方。

                            // 出站：在消息体前追加 4 字节长度头。
                            pipeline.addLast(new LengthFieldPrepender(4));
                            // 出站：把对象编码成字节。
                            pipeline.addLast(new RpcMessageEncoder());

                            // 它的意思是：把 RpcServerRequestHandler 注册为这个连接上的一个入站处理器。
                            pipeline.addLast(new RpcServerRequestHandler(RpcServer.this));
                        }
                    });

            // 绑定端口并阻塞等待服务端通道关闭。
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("服务端启动失败", e);
        } finally {
            // 优雅停机，释放线程与网络资源。
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    RpcResponse invoke(RpcRequest request) {
        RpcResponse response = new RpcResponse();
        // requestId 原样带回，便于请求-响应匹配与日志串联。
        response.setRequestId(request.getRequestId());
        try {
            // 根据接口名定位服务实现。
            Object service = serviceRegistry.getService(request.getInterfaceName());
            if (service == null) {
                throw new IllegalStateException("服务未注册: " + request.getInterfaceName());
            }
            // 用“方法名 + 参数类型”精确定位要执行的方法（支持重载）。
            Method method = service.getClass().getMethod(request.getMethodName(), request.getParameterTypes());// 服务端业务执行
            // 反射执行真实业务方法，args 就是客户端传来的实参。
            Object result = method.invoke(service, request.getArgs());// - 用反射去真正执行目标方法，方法执行完后，把“返回值”接住
            // 把方法返回值写入响应体，供客户端恢复成本地返回值。
            response.setData(result);
            return response;
        } catch (Exception e) {// Day7：如果业务方法自己抛异常，反射层通常会包一层 InvocationTargetException。
            // 这里把真正的业务异常原因展开后再写回客户端，错误语义会更清楚。
            Throwable actualException = e instanceof InvocationTargetException invocationTargetException
                    && invocationTargetException.getTargetException() != null
                    ? invocationTargetException.getTargetException()
                    : e;
            String errorMessage = actualException.getMessage();
            if (errorMessage == null || errorMessage.isBlank()) {
                errorMessage = actualException.getClass().getSimpleName();
            }
            // 统一把异常信息写入 error 字段，客户端据此抛错。
            response.setError(errorMessage);// 服务端把错误写进 RpcResponse.error
            return response;
        }
    }
}
