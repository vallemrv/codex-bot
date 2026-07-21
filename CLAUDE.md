# codex-bot

Bot de Telegram para controlar `codex-cli`. Todo texto visible está en español.

## Verificación

```bash
.venv/bin/python -m py_compile src/*.py
```

## Arquitectura

- `src/telegram_bot.py`: comandos, callbacks, colas y estado en vivo.
- `src/codex_client.py`: subproceso `codex exec --json`, eventos y sesiones.
- `src/db.py`: puntero activo y metadatos persistentes.
- `src/gitops.py`: snapshots y operaciones undo/redo/status.

Codex persiste el historial real en `~/.codex/sessions`. Las columnas SQLite
llamadas `claude_session_id` se conservan internamente para evitar una migración,
pero contienen IDs de conversación de Codex.
