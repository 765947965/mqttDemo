package com.example.mqtt.netty.behavior;

import com.example.mqtt.netty.util.ClientUtil;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connect {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connect.class);

    public void processConnect(Channel channel, MqttConnectMessage connectMessage) {
        LOGGER.info("Start connecting to broker");

        // check client id 检验clientId
        String clientId = connectMessage.payload().clientIdentifier();
        if (!ClientUtil.checkClientId(clientId)) {
            LOGGER.error("Invalid client id!");
            write(channel, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            channel.close();
            return;
        }

        // [MQTT-3.1.3-11] If the User Name Flag is set to 1, this is the next field in the payload.
        // The User Name MUST be a UTF-8 encoded string 校验用户名密码
//        if (connectMessage.variableHeader().hasUserName()) {
//            // TODO: username auth
//        }
        // return connAck msg
        write(channel, MqttConnectReturnCode.CONNECTION_ACCEPTED, true);
        LOGGER.info("Connected : " + channel.hashCode());
    }

    public void disConnect(Channel channel) {
        if (channel.isActive()) {
            channel.close();
        }
        // 连接断开
        LOGGER.info("disConnect : " + channel.hashCode());
    }

    public void processPingReq(Channel channel, MqttMessage connectMessage) {
        // 响应心跳
        MqttMessage pingRespMessage = MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PINGRESP,
                        false, MqttQoS.AT_MOST_ONCE, false, 0),
                null, null);
        channel.writeAndFlush(pingRespMessage);
    }

    private void write(Channel channel, MqttConnectReturnCode connectReturnCode) {
        write(channel, connectReturnCode, false);
    }

    private void write(Channel channel, MqttConnectReturnCode connectReturnCode, boolean sessionPresent) {
        MqttConnAckMessage connAckMessage =
                createConnAckMsg(connectReturnCode, sessionPresent);
        channel.writeAndFlush(connAckMessage);
    }

    private MqttConnAckMessage createConnAckMsg(MqttConnectReturnCode connectReturnCode, boolean sessionPresent) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false,
                MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(connectReturnCode, sessionPresent);
        return new MqttConnAckMessage(mqttFixedHeader, variableHeader
        );
    }

}
