# codex-bot

Control remoto de `codex-cli` por Telegram. Conserva la interfaz de
`claude-bot`: sesiones, proyectos, colas, estado en vivo, cancelación, notas de
voz, archivos y utilidades git.

## Requisitos

- Python 3.11+
- `codex-cli` instalado y autenticado con `codex login`
- Token de Telegram y usuario administrador en `.env`

## Inicio

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./run.sh
```

`src/codex_client.py` ejecuta `codex exec --json`, reanuda conversaciones con
`codex exec resume` y descubre las sesiones de `~/.codex/sessions`.

`PERMISSION_MODE` admite `bypassPermissions`, `workspace-write` y `read-only`.
