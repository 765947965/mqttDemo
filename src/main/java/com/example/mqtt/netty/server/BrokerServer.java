package com.example.mqtt.netty.server;


import com.example.mqtt.netty.config.BrokerConfig;
import com.example.mqtt.netty.handler.BrokerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class BrokerServer {
    private static Logger LOGGER = LoggerFactory.getLogger(BrokerServer.class);

    private BrokerConfig brokerConfig;

    private BrokerHandler brokerHandler;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private Channel channel;

    @Autowired
    public BrokerServer(BrokerConfig brokerConfig, BrokerHandler brokerHandler){
        this.brokerConfig = brokerConfig;
        this.brokerHandler = brokerHandler;
    }

    @PostConstruct
    public void start() throws Exception {
        LOGGER.info("Broker started...");
        LOGGER.info("host: {}", brokerConfig.getPort());
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap sb = new ServerBootstrap().group(bossGroup, workerGroup);
        sb.channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        ChannelPipeline channelPipeline = socketChannel.pipeline();
                        channelPipeline.addFirst("idle",
                                new IdleStateHandler(0, 0, 60));
                        channelPipeline.addLast("decoder", new MqttDecoder());
                        channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                        channelPipeline.addLast("broker", brokerHandler);
                    }
                }).option(ChannelOption.SO_BACKLOG, 1024);
        channel = sb.bind(brokerConfig.getPort()).sync().channel();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        channel.closeFuture().sync();
        LOGGER.info("Broker shut down successfully.");
    }

}
