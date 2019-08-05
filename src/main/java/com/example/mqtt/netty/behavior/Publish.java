package com.example.mqtt.netty.behavior;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Publish {

    private static final Logger LOGGER = LoggerFactory.getLogger(Publish.class);

    public Publish() {

    }

    public void processPublish(Channel channel, MqttPublishMessage publishMessage) {
        // TODO: handle pub to a unconnected broker. After finishing session part
        String topic = publishMessage.variableHeader().topicName();
        ByteBuf buf = publishMessage.content().duplicate();
        byte[] tmp = new byte[buf.readableBytes()];
        buf.readBytes(tmp);
        String content = new String(tmp);
        LOGGER.info(content);
        switch (publishMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE:
                break;
            case AT_LEAST_ONCE:
                sendPublishMsg(channel, topic, "收到");
                break;
            case EXACTLY_ONCE:
                break;
            case FAILURE:
                break;
            default:
                break;
        }

    }

    private void sendPublishMsg(Channel channel, String topic, String message) {
        MqttPublishMessage sendMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPublishVariableHeader(topic, 0),
                Unpooled.buffer().writeBytes(message.getBytes()));
        channel.writeAndFlush(sendMessage);
    }

    private void sendPubAckMsg(Channel channel, int msgId) {
        MqttPubAckMessage pubAckMessage = (MqttPubAckMessage)
                MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBACK,
                                false, MqttQoS.AT_LEAST_ONCE, false, 2),
                       MqttMessageIdVariableHeader.from(msgId), null);
        channel.writeAndFlush(pubAckMessage);
    }

    private void snedPubRecMsg(Channel channel, int msgId) {
        MqttMessage pubRecMessage = MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBREC,
                false, MqttQoS.AT_LEAST_ONCE, false, 2),
                MqttMessageIdVariableHeader.from(msgId), null);
        channel.writeAndFlush(pubRecMessage);
    }

}
