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

## Ventana de contexto

En `~/.codex/models_cache.json`, `context_window` (272K) es el umbral de
facturación de contexto largo, **no** el techo: ese es `max_context_window`
(872K). Codex solo sirve el techo si se lo pides con
`-c model_context_window=…`, y reserva `effective_context_window_percent`
(95%) para la conversación → 828.400 tok reales. Sin ese flag te quedas en
258.400.

`codex exec --json` solo informa de uso al cerrar el turno, así que el
indicador en vivo sondea `token_count` del journal cada 5 s. Ojo: cada
subagente que lanza codex (astra es `multi_agent_version: v2`) escribe su
propio journal con el `session_id` **del padre**; el hilo principal se
identifica por `payload.id == session_id` (`thread_source: "user"`).

Codex persiste el historial real en `~/.codex/sessions`. Las columnas SQLite
llamadas `claude_session_id` se conservan internamente para evitar una migración,
pero contienen IDs de conversación de Codex.
