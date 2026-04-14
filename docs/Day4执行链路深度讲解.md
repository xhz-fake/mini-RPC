# Day4 执行链路深度讲解（服务注册与发现 + 负载均衡 + 最小重试）

## 1. 本阶段目标

Day3 已经解决了连接复用、请求响应精准配对和超时清理，但客户端仍然依赖写死的 `127.0.0.1:9000`。  
Day4 要解决的问题是：

- 服务地址不再写死在客户端代码中
- 服务端启动后能自动“上报自己在哪”
- 客户端能先按**服务名**发现实例，再决定连接哪台机器
- 实例连接失败时，客户端具备**最小可用重试能力**

## 2. Day4 新增了哪些核心对象

- `ServiceInstance`：描述一个**服务实例**，包含 `serviceName/host/port`
- `RegistryCenter`：**注册中心抽象**，定义注册、注销、发现能力
- `FileRegistryCenter`：本地文件注册中心，实现**跨进程共享实例**信息
- `LoadBalancer`：**负载均衡**接口
- `RandomLoadBalancer`：**随机负载均衡**实现

## 3. 服务端启动链路新增了什么

服务端入口 `ServerBootstrap` 在原有本地 `ServiceRegistry` 之外，又增加了外部注册中心步骤：

1. 读取 `rpc.server.host` 和 `rpc.server.port`
2. 构造 `ServiceInstance`
3. 调用 `FileRegistryCenter.register(instance)` 写入注册中心文件
4. 打印 `service registered: ...`
5. 在 JVM 关闭时执行 `unregister(instance)`

这意味着服务端不仅能处理请求，还能告诉客户端“我现在可用，地址在这里”。

## 4. 客户端调用链路新增了什么

客户端入口 `ClientBootstrap` 不再写死 IP/端口，而是构造：

- `FileRegistryCenter`
- `RandomLoadBalancer`
- `RpcClient(registryCenter, loadBalancer)`

真正发请求时，`RpcClient` 会新增 4 个动作：

1. 根据 `request.interfaceName` 调 `registryCenter.discover(serviceName)`
2. 拿到实例列表后，通过 `loadBalancer.select(...)` 选出目标实例
3. 按实例地址维护 `endpoint -> channel` 连接缓存
4. 在连接/发送失败时切换到其他实例做最小重试

## 5. Day4 请求发送新主线

`本地方法调用`
→ `代理组装 RpcRequest`
→ `注册中心按服务名发现实例`
→ `负载均衡选中目标实例`
→ `按实例地址复用或新建连接`
→ `请求发送`
→ `服务端处理并回包`
→ `客户端按 requestId 完成 future`

与 Day3 相比，新增的核心层就是：

`服务发现 + 实例选择 + 多实例连接管理`

## 6. 为什么连接缓存从单个 channel 升级成 Map

Day3 只有一个固定服务端地址，所以一个 `channel` 就够了。  
Day4 有多个候选实例后，客户端可能会连到 `127.0.0.1:9000`，也可能连到 `127.0.0.1:9010`。因此连接缓存必须升级成：

`endpoint(host:port) -> channel`

这样同一个客户端就能分别复用不同实例上的连接。

## 7. 为什么挂起请求也要记录 endpoint

Day3 的断连清理默认会把所有 pending 请求都失败回填。  
Day4 存在多实例连接后，如果只是一条连接断了，不应该误伤发往其他实例的正常请求。

所以 Day4 的挂起请求上下文增加了 `endpointKey`，含义是：

- 这个请求是发往哪个实例的
- 如果某条连接断开，只清理属于该实例的 pending 请求

## 8. Day4 最小重试是怎么做的

Day4 的重试不是无限重试，而是最小可用策略：

- 默认最多 2 次尝试
- 第一次失败后，从“尚未尝试过的实例”里再选一个
- 如果没有剩余实例，直接失败返回

这样设计的目的是：

- 先把“失败切换实例”这条最小闭环跑通
- 避免一开始把幂等、指数退避、熔断这些复杂问题全部引入

## 9. 本地文件注册中心的定位

`FileRegistryCenter` 不是生产级注册中心，它是 Day4 的教学型实现。  
它解决的是“先把注册发现主链路跑通”这个问题，优点是：

- 不依赖外部中间件
- 可直接本地多进程验证
- 后续替换成 Zookeeper 时，上层接口基本不用大改

## 10. Day4 调试重点

建议优先看这几个位置：

1. `ServerBootstrap`：实例何时注册
2. `FileRegistryCenter#register`：注册中心文件何时写入
3. `RpcClient#selectServiceInstance`：客户端何时发现实例
4. `RandomLoadBalancer#select`：具体选中了哪个实例
5. `RpcClient#ensureConnected(ServiceInstance)`：按实例建立或复用连接
6. `RpcClientResponseHandler#channelRead0`：响应仍然如何按 requestId 配对

## 11. 关键链路细讲：从 writeAndFlush 到服务端 channelRead0

这一段是 Day4 里最容易“听懂概念，但看不清执行过程”的部分。  
下面只抓关键位置细讲，不重要的底层细节一笔带过。

### 11.1 起点：客户端手里的数据还只是 Java 对象

在 `RpcClient#sendRequest` 里，客户端最终会执行：

```java
activeChannel.writeAndFlush(attemptRequest).sync();
```

此时的 `attemptRequest` 还不是网络字节，而是一个 `RpcRequest` 对象。  
它里面装的是：

- `requestId`
- `interfaceName`
- `methodName`
- `parameterTypes`
- `args`

也就是说，这一刻客户端只是把“一个 Java 对象”交给 Netty，请 Netty 帮忙把它发出去。

### 11.2 客户端出站：对象先变成字节

客户端 pipeline 中，真正和“发送请求”相关的关键处理器是：

- `RpcMessageEncoder`
- `LengthFieldPrepender`

先看 `RpcMessageEncoder`。它会调用：

```java
RpcMessageCodec.encode(msg)
```

这一步的本质是：

`RpcRequest 对象 -> byte[]`

也就是把请求对象序列化成一串字节。  
注意：从这里开始，网络里传输的就不再是 Java 对象，而是二进制字节。

### 11.3 客户端出站：再补 4 字节长度头

请求对象编码成字节后，还会经过 `LengthFieldPrepender(4)`。  
它的作用是：

- 在消息体前面补 4 个字节
- 这 4 个字节表示“后面的正文长度是多少”

于是数据形态变成：

`[4字节长度头][消息体字节]`

为什么需要它？

- 因为 TCP 只保证“字节流传输”，不保证“消息边界”
- 服务端如果不知道一条消息有多长，就不知道该从字节流里切出多少内容

所以这 4 字节长度头，其实是在告诉服务端：

```text
请先看前 4 个字节，它决定后面的正文有多长
```

### 11.4 网络传输：这段不是项目业务代码

补完长度头后，Netty 会把这些字节交给底层 Socket。  
再往后就是：

- Netty
- Java NIO
- 操作系统 TCP 协议栈
- 网卡

共同把字节发到服务端。

这里要特别记住一句：

`网络中传输的永远是字节，不是 Java 对象`

### 11.5 服务端入站：先按长度切出完整一帧

服务端收到的第一手数据也是字节流，不是 `RpcRequest`。  
它先进入服务端 pipeline 中的：

- `LengthFieldBasedFrameDecoder`

这个解码器会：

- 先读取前 4 个字节
- 得到正文长度
- 再按这个长度切出“恰好一整条消息”

它解决的是：

- 粘包
- 半包

问题。

走完这一步后，后面的处理器拿到的已经不是混乱字节流，而是一整条完整消息体。

### 11.6 服务端入站：字节再还原成 RpcRequest 对象

接下来进入 `RpcMessageDecoder`。  
它会做两件事：

1. 从 `ByteBuf` 中读出完整消息字节
2. 调用 `RpcMessageCodec.decode(bytes)` 反序列化

这一步的数据变化是：

`byte[] -> RpcRequest 对象`

所以到这里，服务端才第一次真正重新得到一个可以直接使用的 Java 请求对象。

### 11.7 最终进入业务入口：RpcServerRequestHandler#channelRead0

当前面的拆帧和解码都完成后，Netty 才会把对象交给：

- `RpcServerRequestHandler#channelRead0(ChannelHandlerContext ctx, RpcRequest request)`

这时方法参数里的 `request`，已经是服务端刚刚通过反序列化还原出来的对象了。  
也就是说，`channelRead0(...)` 不是“收到原始网络字节”的地方，而是：

`服务端正式拿到解码完成后的 RpcRequest 对象的业务入口`

### 11.8 这一整段最该记住的执行顺序

可以把这条链路背成：

`RpcClient.writeAndFlush(request)`
→ `RpcMessageEncoder.encode`
→ `RpcMessageCodec.encode`
→ `LengthFieldPrepender`
→ `TCP网络传输`
→ `LengthFieldBasedFrameDecoder`
→ `RpcMessageDecoder.decode`
→ `RpcMessageCodec.decode`
→ `RpcServerRequestHandler.channelRead0`

### 11.9 这一整段最该记住的数据变化

也可以把数据形态记成：

`RpcRequest对象`
→ `byte[]`
→ `[长度头 + 消息体字节]`
→ `TCP字节流`
→ `完整消息体字节`
→ `RpcRequest对象`

## 12. 响应半条链：从服务端回写到客户端 future 完成

请求半条链打通后，响应半条链其实是“镜像过程”。

### 12.1 服务端业务执行完后先得到 RpcResponse

在 `RpcServer#invoke` 中：

- 根据接口名找到服务实现
- 根据方法名和参数类型找到目标方法
- 执行 `method.invoke(...)`
- 把返回值塞进 `RpcResponse`

然后在 `RpcServerRequestHandler#channelRead0` 中执行：

```java
ctx.writeAndFlush(response);
```

### 12.2 服务端响应出站的关键流程

服务端回写响应时也会经过：

- `RpcMessageEncoder`
- `LengthFieldPrepender`

所以响应也会经历：

`RpcResponse对象 -> byte[] -> [长度头 + 消息体字节] -> TCP`

### 12.3 客户端接收响应的关键流程

客户端收到响应字节后，会先经过：

- `LengthFieldBasedFrameDecoder`
- `RpcMessageDecoder`

于是又把网络字节还原成 `RpcResponse` 对象。

### 12.4 客户端真正完成闭环的位置

最后消息会进入：

- `RpcClientResponseHandler#channelRead0`

这里会：

1. 用 `response.requestId` 去 `pendingRequests` 里找对应挂起请求
2. 拿到对应的 `future`
3. 调 `future.complete(response)`

这一步就是整个 Day3/Day4 请求响应闭环最关键的地方。  
它的意义是：

`服务端回包 -> 命中正确 requestId -> 唤醒正确等待线程`

### 12.5 为什么客户端代码能像同步调用一样拿到返回值

因为发送请求后，客户端主线程已经在：

```java
future.get(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
```

这里阻塞等待。

当 `RpcClientResponseHandler` 里执行了 `future.complete(response)` 后：

- 阻塞线程被唤醒
- `sendRequest(...)` 返回 `RpcResponse`
- 动态代理再把 `response.getData()` 返回给调用方

所以调用方看起来像：

```java
String result = helloService.hello("mini-RPC-1");
```

但底层其实走的是：

`异步网络通信 + future 阻塞等待 + requestId 精准回填`

## 13. Day4 面试一句话总结

Day4 解决的是“服务在哪”和“失败后怎么办”的最小版本问题：  
服务端启动时自动注册实例，客户端调用前先按服务名发现实例，再通过负载均衡选择目标地址并建立连接；在实例连接失败时，客户端可以切换到其他实例做最小重试。这样项目从固定地址直连，进化到了具备基础服务治理能力的 RPC 雏形。
