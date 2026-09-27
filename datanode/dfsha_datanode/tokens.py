"""Verificación de tokens de bloque emitidos por el ControlNode (HMAC-SHA256).

Formato:  base64url(blockId|usuario|modo|expEpochSeg) + "." + base64url(HMAC)
El DataNode no consulta la base de usuarios: sólo comprueba la firma, el bloque,
el modo y la expiración. Es el mismo mecanismo de los block tokens de HDFS.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import time


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def verify(token: str, block_id: str, mode: str, secret: str) -> tuple[bool, str]:
    """Devuelve (válido, motivo)."""
    if not token:
        return False, "token ausente"
    parts = token.split(".")
    if len(parts) != 2:
        return False, "formato inválido"
    payload_b64, signature = parts
    expected = _b64(hmac.new(secret.encode(), payload_b64.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(expected, signature):
        return False, "firma inválida"
    try:
        fields = _unb64(payload_b64).decode().split("|")
        token_block, user, token_mode, expires = fields
    except (ValueError, UnicodeDecodeError):
        return False, "payload inválido"
    if token_block != block_id:
        return False, "el token es de otro bloque"
    if token_mode != mode:
        return False, f"el token no autoriza el modo '{mode}'"
    if int(expires) < int(time.time()):
        return False, "token expirado"
    return True, user
