"""Sesión local del cliente: URL del ControlNode, token JWT y directorio actual."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path

SESSION_PATH = Path(os.getenv("DFSHA_SESSION", str(Path.home() / ".dfsha" / "session.json")))


@dataclass
class Session:
    control_node: str = os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8080")
    token: str | None = None
    username: str | None = None
    cwd: str = "/"

    @classmethod
    def load(cls) -> "Session":
        if SESSION_PATH.exists():
            try:
                data = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
                session = cls(**{k: v for k, v in data.items() if k in cls.__annotations__})
                if os.getenv("DFSHA_CONTROLNODE_URL"):
                    session.control_node = os.environ["DFSHA_CONTROLNODE_URL"]
                return session
            except (OSError, ValueError, TypeError):
                pass
        return cls()

    def save(self) -> None:
        SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
        SESSION_PATH.write_text(json.dumps(asdict(self), indent=2), encoding="utf-8")
        os.chmod(SESSION_PATH, 0o600)

    def resolve(self, path: str | None) -> str:
        """Convierte una ruta relativa en absoluta usando el directorio actual."""
        if not path or path == ".":
            return self.cwd
        base = path if path.startswith("/") else f"{self.cwd.rstrip('/')}/{path}"
        parts: list[str] = []
        for part in base.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return "/" + "/".join(parts)
