package io.dfsha.controlnode.security;

import io.dfsha.controlnode.config.ControlNodeProperties;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Tokens de bloque: el ControlNode autoriza una operación concreta sobre un
 * bloque y el DataNode sólo verifica la firma. Así el control de acceso llega
 * al plano de datos sin que el DataNode consulte la base de usuarios.
 *
 * Formato:  base64url(blockId|usuario|modo|expEpochSeg) + "." + base64url(HMAC-SHA256)
 */
@Service
public class BlockTokenService {

    public static final String MODE_READ = "r";
    public static final String MODE_WRITE = "w";

    private final byte[] secret;
    private final long ttlSeconds;

    public BlockTokenService(ControlNodeProperties props) {
        this.secret = props.blockTokenSecret().getBytes(StandardCharsets.UTF_8);
        this.ttlSeconds = props.blockTokenTtlSeconds();
    }

    public String issue(String blockId, String user, String mode) {
        long exp = System.currentTimeMillis() / 1000 + ttlSeconds;
        String payload = blockId + "|" + user + "|" + mode + "|" + exp;
        String p64 = b64(payload.getBytes(StandardCharsets.UTF_8));
        return p64 + "." + b64(hmac(p64.getBytes(StandardCharsets.UTF_8)));
    }

    /** Token para un DataNode que debe copiar un bloque a otro DataNode (re-replicación). */
    public String issueForNode(String blockId, String nodeId) {
        return issue(blockId, "node:" + nodeId, MODE_WRITE);
    }

    public boolean verify(String token, String blockId, String mode) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length != 2) return false;
            if (!b64(hmac(parts[0].getBytes(StandardCharsets.UTF_8))).equals(parts[1])) return false;
            String[] f = new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8).split("\\|");
            return f.length == 4
                    && f[0].equals(blockId)
                    && f[2].equals(mode)
                    && Long.parseLong(f[3]) >= System.currentTimeMillis() / 1000;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private byte[] hmac(byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo calcular el HMAC del token de bloque", e);
        }
    }

    private static String b64(byte[] b) { return Base64.getUrlEncoder().withoutPadding().encodeToString(b); }
}
