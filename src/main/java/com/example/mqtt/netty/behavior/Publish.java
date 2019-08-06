package com.example.mqtt.netty.behavior;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Publish {

    private static final Logger LOGGER = LoggerFactory.getLogger(Publish.class);

    public void processPublish(Channel channel, MqttPublishMessage publishMessage) {
        // 消息质量处理(回应)
        int messageId = publishMessage.variableHeader().messageId();
        switch (publishMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE:
                break;
            case AT_LEAST_ONCE:
                sendPubAckMsg(channel, messageId);
                break;
            case EXACTLY_ONCE:
                sendPubRecMsg(channel, messageId);//TODO 未完善
                break;
            default:
                break;
        }

        String topic = publishMessage.variableHeader().topicName();
        ByteBuf buf = publishMessage.content().duplicate();
        byte[] tmp = new byte[buf.readableBytes()];
        buf.readBytes(tmp);
        String content = new String(tmp);
        LOGGER.info(content);
        sendPublishMsg(channel, topic, "收到");
    }

    private void sendPublishMsg(Channel channel, String topic, String message) {
        MqttPublishMessage sendMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPublishVariableHeader(topic, 0),
                Unpooled.buffer().writeBytes(message.getBytes()));
        channel.writeAndFlush(sendMessage);
    }

    /**
     * 发送qos1 publish  确认消息
     */
    private void sendPubAckMsg(Channel channel, int msgId) {
        MqttPubAckMessage pubAckMessage = (MqttPubAckMessage)
                MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBACK,
                                false, MqttQoS.AT_LEAST_ONCE, false, 2),
                        MqttMessageIdVariableHeader.from(msgId), null);
        channel.writeAndFlush(pubAckMessage);
    }


    /**
     * 发送qos2 publish  确认消息 第一步
     */
    private void sendPubRecMsg(Channel channel, int msgId) {
        MqttMessage pubRecMessage = MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBREC,
                        false, MqttQoS.AT_LEAST_ONCE, false, 2),
                MqttMessageIdVariableHeader.from(msgId), null);
        channel.writeAndFlush(pubRecMessage);
    }

}
