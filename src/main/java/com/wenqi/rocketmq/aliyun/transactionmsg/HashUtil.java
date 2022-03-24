package com.wenqi.rocketmq.aliyun.transactionmsg;

import java.util.zip.CRC32;

/**
 * @author liangwenqi
 * @date 2022/3/24
 */
public class HashUtil {
    public static long crc32Code(byte[] bytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return crc32.getValue();
    }
}
