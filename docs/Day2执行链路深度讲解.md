# Day2 执行链路深度讲解（Netty 版）

## 1. 讲解目标

只围绕一个目标理解 Day2：

`客户端通过 Netty 完成远程调用，并输出 rpc result: hello mini-RPC`

讲解顺序继续采用“执行链路深挖”，不是按模块目录讲。

## 2. 先看 Day2 相比 Day1 到底换了什么

不变的部分：

- RpcRequest / RpcResponse 协议不变
- 动态代理拦截逻辑不变
- 服务端 invoke 反射执行逻辑不变

变化的部分：

- 传输层从 Socket + InputStream/OutputStream 变成 Netty ChannelPipeline
- 服务端并发模型从手写线程池变成 Netty boss/worker EventLoop

一句话：  
Day2 是“**只替换通信实现，不改业务语义**”。

## 3. 深度优先追踪（从调用点一路钻到底）

### 第 1 层：调用入口不变

客户端还是从 `helloService.hello("mini-RPC")` 发起调用。  
代理层依然会组装 RpcRequest 并调用 `RpcClient#sendRequest`。

这一步告诉你：  
Netty 改造没有破坏上层调用体验。

### 第 2 层：客户端如何用 Netty 发请求

进入 `RpcClient#sendRequest` 后，核心动作是：

1. 创建客户端事件循环组 `NioEventLoopGroup(1)`  
2. 配置 `Bootstrap`，指定 `NioSocketChannel`  
3. 构建 pipeline（解帧、解码、封帧、编码、响应处理器）  
4. `connect` 建连  
5. `writeAndFlush(request)` 发请求  
6. 等待响应处理器返回 RpcResponse  

重点看 pipeline 顺序：

- `LengthFieldBasedFrameDecoder`
- `RpcMessageDecoder`
- `LengthFieldPrepender`
- `RpcMessageEncoder`
- `RpcClientResponseHandler`

这代表两条方向：

- 入站（服务端回包）：先拆帧，再转对象
- 出站（客户端发包）：先转字节，再加长度头

### 第 3 层：客户端为什么还能“同步拿结果”

`RpcClientResponseHandler` 内部用了 `CountDownLatch`：

- 收到 `RpcResponse` 时保存到字段并 `countDown`
- 发送线程在 `awaitResponse()` 里阻塞等待

这就是 Day2 的关键过渡技巧：  
底层是异步事件驱动，上层仍保持同步调用语义。

### 第 4 层：服务端如何用 Netty 接入请求

`RpcServer#start` 的核心动作：

1. 创建 `bossGroup(1)`：专门负责接入连接  
2. 创建 `workerGroup`：专门负责 IO 读写和 handler 执行  
3. 配置 `ServerBootstrap` 并绑定端口  
4. 初始化子通道 pipeline

服务端 pipeline 与客户端对称：

- `LengthFieldBasedFrameDecoder`
- `RpcMessageDecoder`
- `LengthFieldPrepender`
- `RpcMessageEncoder`
- `RpcServerRequestHandler`

### 第 5 层：服务端业务执行如何衔接

进入 `RpcServerRequestHandler#channelRead0`：

1. 拿到已经解码好的 `RpcRequest`
2. 调用 `rpcServer.invoke(request)` 执行本地方法
3. `ctx.writeAndFlush(response)` 回写响应

而 `invoke` 逻辑与 Day1 一致：

- 接口名找服务实例
- 方法名 + 参数类型定位方法
- 反射执行并写入 response.data

这说明：  
Day2 改的是“怎么收发”，不是“怎么执行业务”。

## 4. 你现在该建立的 Day2 脑内模型

`本地方法调用`
→ `代理封装 RpcRequest`
→ `Netty 客户端 pipeline 编码并发送`
→ `Netty 服务端 pipeline 解码并路由`
→ `invoke 反射执行业务`
→ `Netty 服务端编码回包`
→ `Netty 客户端解码响应`
→ `CountDownLatch 唤醒等待线程`
→ `返回结果并打印`

## 5. Day2 关键代码跳转（按执行顺序）

1. 客户端发送入口：`RpcClient#sendRequest`
2. 客户端响应处理：`RpcClientResponseHandler`
3. 客户端编码器：`RpcMessageEncoder`
4. 客户端解码器：`RpcMessageDecoder`
5. 服务端启动与管线：`RpcServer#start`
6. 服务端入站处理：`RpcServerRequestHandler#channelRead0`
7. 服务端业务执行：`RpcServer#invoke`

## 6. Day2 自测练习（确认你真的懂了）

### 练习 1：看懂“异步外壳 + 同步语义”

在 `RpcClientResponseHandler#channelRead0` 打断点，观察响应到达后 `latch` 如何唤醒 `awaitResponse`。

### 练习 2：验证长度帧必要性

临时去掉 `LengthFieldPrepender`，再调用一次，观察解码异常，理解“消息边界”为什么关键。

### 练习 3：验证业务层未被 Netty 改造影响

修改 `HelloServiceImpl#hello` 的返回值，调用仍可成功，说明 Netty 改造仅在通信层。
