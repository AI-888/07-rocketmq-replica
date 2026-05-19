package org.apache.rocketmq.hasync.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclUtils extends org.apache.rocketmq.acl.common.AclUtils {
    private static final Logger log = LoggerFactory.getLogger(AclUtils.class);

    public static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        if (!StringUtils.isBlank(accessKey) && !StringUtils.isBlank(secretKey)) {
            return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        } else {
            log.warn("accessKey or secretKey is blank, rpc hook is null");
            return null;
        }
    }
}
