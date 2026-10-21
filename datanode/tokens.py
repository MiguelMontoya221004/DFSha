"""Verificacion de los tokens de bloque que emite el ControlNode.

El ControlNode firma un permiso para UNA operacion sobre UN bloque y el DataNode
solo recalcula la firma con el secreto compartido: asi el control de acceso
llega al plano de datos sin que este nodo tenga que consultar usuarios.

Formato (Auth.java del ControlNode):

    <blockId>:<usuario>:<op>:<vence>:<firma>

donde <op> es "read" o "write", <vence> son milisegundos desde el epoch y
<firma> es el HMAC-SHA256 en base64url (sin relleno) de todo lo anterior.
"""
import base64
import hashlib
import hmac
import time

LEER = "read"
ESCRIBIR = "write"


def _firmar(texto: str, secreto: str) -> str:
    firma = hmac.new(secreto.encode("utf-8"), texto.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(firma).decode().rstrip("=")


def verificar(token: str, block_id: str, op: str, secreto: str):
    """Devuelve (es_valido, usuario_o_motivo_del_rechazo)."""
    if not token:
        return False, "falta el token de bloque"

    cuerpo, _, firma = token.rpartition(":")
    if not cuerpo or not firma:
        return False, "el token no tiene el formato esperado"

    if not hmac.compare_digest(_firmar(cuerpo, secreto), firma):
        return False, "firma invalida"

    partes = cuerpo.split(":")
    if len(partes) < 4:
        return False, "contenido del token incompleto"
    # El usuario podria traer ":", asi que se arma con lo que queda en el medio
    token_block = partes[0]
    vence = partes[-1]
    token_op = partes[-2]
    usuario = ":".join(partes[1:-2])

    if token_block != block_id:
        return False, "el token es de otro bloque"
    if token_op != op:
        return False, f"el token no autoriza la operacion '{op}'"
    try:
        if int(vence) < int(time.time() * 1000):
            return False, "token vencido"
    except ValueError:
        return False, "fecha de vencimiento ilegible"

    return True, usuario
