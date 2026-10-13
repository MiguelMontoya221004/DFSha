"""Verificación de los tokens de bloque que emite el ControlNode.

El ControlNode firma una autorización para una operación concreta sobre un
bloque y el DataNode solo comprueba la firma: así el control de acceso llega al
plano de datos sin que este nodo tenga que consultar la base de usuarios.

Formato del token:
    base64url(blockId|usuario|modo|expiracion) + "." + base64url(HMAC-SHA256)
"""
import base64
import hashlib
import hmac
import time

MODO_LECTURA = "r"
MODO_ESCRITURA = "w"


def _b64(datos: bytes) -> str:
    return base64.urlsafe_b64encode(datos).decode().rstrip("=")


def _desde_b64(texto: str) -> bytes:
    # El token viaja sin "=" al final, hay que devolvérselos para decodificar
    return base64.urlsafe_b64decode(texto + "=" * (-len(texto) % 4))


def verificar(token: str, block_id: str, modo: str, secreto: str):
    """Devuelve (es_valido, usuario_o_motivo_del_rechazo)."""
    if not token:
        return False, "falta el token de bloque"

    partes = token.split(".")
    if len(partes) != 2:
        return False, "el token no tiene el formato esperado"

    cuerpo, firma = partes
    firma_esperada = _b64(
        hmac.new(secreto.encode(), cuerpo.encode(), hashlib.sha256).digest()
    )
    if not hmac.compare_digest(firma_esperada, firma):
        return False, "firma invalida"

    try:
        token_block, usuario, token_modo, expira = _desde_b64(cuerpo).decode().split("|")
    except (ValueError, UnicodeDecodeError):
        return False, "contenido del token ilegible"

    if token_block != block_id:
        return False, "el token es de otro bloque"
    if token_modo != modo:
        return False, f"el token no autoriza el modo '{modo}'"
    if int(expira) < int(time.time()):
        return False, "token expirado"

    return True, usuario
