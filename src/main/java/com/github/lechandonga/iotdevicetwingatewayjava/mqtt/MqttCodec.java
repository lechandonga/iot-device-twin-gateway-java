package com.github.lechandonga.iotdevicetwingatewayjava.mqtt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * MQTT 3.1.1 最小编解码工具（仅实现网关所需的报文子集）。
 */
final class MqttCodec {

    private MqttCodec() {
    }

    static byte readByte(InputStream in) throws IOException {
        int value = in.read();
        if (value < 0) {
            throw new IOException("连接已关闭");
        }
        return (byte) value;
    }

    static byte[] readRemaining(InputStream in, int length) throws IOException {
        byte[] buffer = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = in.read(buffer, offset, length - offset);
            if (read < 0) {
                throw new IOException("连接已关闭");
            }
            offset += read;
        }
        return buffer;
    }

    static int decodeRemainingLength(InputStream in) throws IOException {
        int multiplier = 1;
        int value = 0;
        int encoded;
        do {
            encoded = readByte(in) & 0xFF;
            value += (encoded & 0x7F) * multiplier;
            multiplier *= 128;
            if (multiplier > 128 * 128 * 128) {
                throw new IOException("剩余长度非法");
            }
        } while ((encoded & 0x80) != 0);
        return value;
    }

    static byte[] encodeRemainingLength(int length) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        do {
            int encoded = length % 128;
            length /= 128;
            if (length > 0) {
                encoded |= 0x80;
            }
            out.write(encoded);
        } while (length > 0);
        return out.toByteArray();
    }

    static byte[] packet(int typeAndFlags, byte[] variableHeaderAndPayload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(typeAndFlags);
        try {
            out.write(encodeRemainingLength(variableHeaderAndPayload.length));
            out.write(variableHeaderAndPayload);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    static byte[] encodeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new byte[]{(byte) (bytes.length >> 8), (byte) bytes.length};
    }

    static String decodeString(byte[] buffer, int[] cursor) {
        int length = ((buffer[cursor[0]] & 0xFF) << 8) | (buffer[cursor[0] + 1] & 0xFF);
        cursor[0] += 2;
        String value = new String(buffer, cursor[0], length, StandardCharsets.UTF_8);
        cursor[0] += length;
        return value;
    }

    static byte[] publishPacket(String topic, byte[] payload, int packetId) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(encodeString(topic), 0, 2);
        out.write(topicBytes, 0, topicBytes.length);
        out.write((byte) (packetId >> 8));
        out.write((byte) packetId);
        out.write(payload, 0, payload.length);
        return packet(0x32, out.toByteArray());
    }

    static byte[] connackPacket(boolean sessionPresent, byte returnCode) {
        return packet(0x20, new byte[]{(byte) (sessionPresent ? 1 : 0), returnCode});
    }

    static byte[] pubackPacket(int packetId) {
        return packet(0x40, new byte[]{(byte) (packetId >> 8), (byte) packetId});
    }

    static byte[] subackPacket(int packetId, byte grantedQos) {
        return packet(0x90, new byte[]{(byte) (packetId >> 8), (byte) packetId, grantedQos});
    }

    static final byte[] PINGRESP = packet(0xD0, new byte[0]);
}
