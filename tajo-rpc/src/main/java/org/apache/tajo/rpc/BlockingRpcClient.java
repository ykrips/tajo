/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.rpc;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcProtos.RpcRequest;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;
import org.apache.tajo.util.NetUtils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ChannelHandler.Sharable;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.rpc.RpcConnectionPool.RpcConnectionKey;

public class BlockingRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final ChannelInboundHandler handler;
  private final ChannelInitializer<Channel> initializer;
  private final ProxyRpcChannel rpcChannel;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final Map<Integer, ProtoCallFuture> requests =
      new ConcurrentHashMap<Integer, ProtoCallFuture>();

  private final Class<?> protocol;
  private final Method stubMethod;

  private RpcConnectionKey key;

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   */
  BlockingRpcClient(final Class<?> protocol,
                           final InetSocketAddress addr, EventLoopGroup loopGroup, int retries)
      throws ClassNotFoundException, NoSuchMethodException, ConnectTimeoutException {

    this.protocol = protocol;
    String serviceClassName = protocol.getName() + "$"
        + protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    stubMethod = serviceClass.getMethod("newBlockingStub",
        BlockingRpcChannel.class);

    this.handler = new ClientChannelInboundHandler();
    initializer = new ProtoChannelInitializer(handler,
        RpcResponse.getDefaultInstance());
    super.init(addr, initializer, loopGroup, retries);
    rpcChannel = new ProxyRpcChannel();

    this.key = new RpcConnectionKey(addr, protocol, false);
  }

  @Override
  public RpcConnectionKey getKey() {
    return key;
  }

  @Override
  public <T> T getStub() {
    try {
      return (T) stubMethod.invoke(null, rpcChannel);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public BlockingRpcChannel getBlockingRpcChannel() {
    return this.rpcChannel;
  }

  private class ProxyRpcChannel implements BlockingRpcChannel {

    private final ClientChannelInboundHandler handler;

    public ProxyRpcChannel() {

      this.handler = getChannel().pipeline().
          get(ClientChannelInboundHandler.class);

      if (handler == null) {
        throw new IllegalArgumentException("Channel does not have " +
            "proper handler");
      }
    }

    @Override
    public Message callBlockingMethod(final MethodDescriptor method,
                                      final RpcController controller,
                                      final Message param,
                                      final Message responsePrototype)
        throws TajoServiceException {

      int nextSeqId = sequence.getAndIncrement();

      Message rpcRequest = buildRequest(nextSeqId, method, param);

      ProtoCallFuture callFuture =
          new ProtoCallFuture(controller, responsePrototype);
      requests.put(nextSeqId, callFuture);
      getChannel().writeAndFlush(rpcRequest);

      try {
        return callFuture.get();
      } catch (Throwable t) {
        if (t instanceof ExecutionException) {
          Throwable cause = t.getCause();
          if (cause != null && cause instanceof TajoServiceException) {
            throw (TajoServiceException)cause;
          }
        }
        throw new TajoServiceException(t.getMessage());
      }
    }

    private Message buildRequest(int seqId,
                                 MethodDescriptor method,
                                 Message param) {
      RpcRequest.Builder requestBuilder = RpcRequest.newBuilder()
          .setId(seqId)
          .setMethodName(method.getName());

      if (param != null) {
        requestBuilder.setRequestMessage(param.toByteString());
      }

      return requestBuilder.build();
    }
  }

  private String getErrorMessage(String message) {
    if(protocol != null && getChannel() != null) {
      return protocol.getName() +
          "(" + RpcUtils.normalizeInetSocketAddress((InetSocketAddress)
          getChannel().getRemoteAddress()) + "): " + message;
    } else {
      return "Exception " + message;
    }
  }

  private TajoServiceException makeTajoServiceException(RpcResponse response, Throwable cause) {
    if(protocol != null && getChannel() != null) {
      return new TajoServiceException(response.getErrorMessage(), cause, protocol.getName(),
          RpcUtils.normalizeInetSocketAddress((InetSocketAddress)getChannel().getRemoteAddress()));
    } else {
      return new TajoServiceException(response.getErrorMessage());
    }
  }

  @Sharable
  private class ClientChannelInboundHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {

      if (msg instanceof RpcResponse) {
        RpcResponse rpcResponse = (RpcResponse) msg;
        ProtoCallFuture callback = requests.remove(rpcResponse.getId());

        if (callback == null) {
          LOG.warn("Dangling rpc call");
        } else {
          if (rpcResponse.hasErrorMessage()) {
            callback.setFailed(rpcResponse.getErrorMessage(),
                makeTajoServiceException(rpcResponse, new ServiceException(rpcResponse.getErrorTrace())));
            throw new RemoteException(getErrorMessage(rpcResponse.getErrorMessage()));
          } else {
            Message responseMessage;

            if (!rpcResponse.hasResponseMessage()) {
              responseMessage = null;
            } else {
              responseMessage = callback.returnType.newBuilderForType().mergeFrom(rpcResponse.getResponseMessage())
                  .build();
            }

            callback.setResponse(responseMessage);
          }
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      ctx.close();
      for(ProtoCallFuture callback: requests.values()) {
        callback.setFailed(cause.getMessage(), cause);
      }
      if(LOG.isDebugEnabled()) {
        LOG.error("" + cause.getMessage(), cause);
      } else {
        LOG.error("RPC Exception:" + cause.getMessage());
      }
    }
  }

 static class ProtoCallFuture implements Future<Message> {
    private Semaphore sem = new Semaphore(0);
    private Message response = null;
    private Message returnType;

    private RpcController controller;

    private ExecutionException ee;

    public ProtoCallFuture(RpcController controller, Message message) {
      this.controller = controller;
      this.returnType = message;
    }

    @Override
    public boolean cancel(boolean arg0) {
      return false;
    }

    @Override
    public Message get() throws InterruptedException, ExecutionException {
      sem.acquire();
      if(ee != null) {
        throw ee;
      }
      return response;
    }

    @Override
    public Message get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if(sem.tryAcquire(timeout, unit)) {
        return response;
      } else {
        throw new TimeoutException();
      }
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return sem.availablePermits() > 0;
    }

    public void setResponse(Message response) {
      this.response = response;
      sem.release();
    }

    public void setFailed(String errorText, Throwable t) {
      if(controller != null) {
        this.controller.setFailed(errorText);
      }
      ee = new ExecutionException(errorText, t);
      sem.release();
    }
  }
}