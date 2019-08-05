package com.example.mqtt.netty.handler;

import com.example.mqtt.netty.behavior.Connect;
import com.example.mqtt.netty.behavior.Publish;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private Connect connect;

    private Publish publish;

    @Autowired
    public BrokerHandler(Connect connect, Publish publish) {
        this.connect = connect;
        this.publish = publish;
    }

    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
         switch (msg.fixedHeader().messageType()) {
             case CONNECT:
                 connect.processConnect(ctx.channel(), (MqttConnectMessage) msg);
                 break;
             case CONNACK:
                 break;
             case PUBLISH:
                 publish.processPublish(ctx.channel(), (MqttPublishMessage) msg);
                 break;
             case PUBACK:
                 break;
             case PUBREC:
                 break;
             case PUBREL:
                 break;
             case PUBCOMP:
                 break;
             case SUBSCRIBE:
                 break;
             case SUBACK:
                 break;
             case UNSUBSCRIBE:
                 break;
             case UNSUBACK:
                 break;
             case PINGREQ:
                 break;
             case PINGRESP:
                 break;
             case DISCONNECT:
                 break;
             default:
                 break;
         }
    }


}
