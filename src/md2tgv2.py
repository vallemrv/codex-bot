"""
md2tgv2.py — Converts LLM Markdown to Telegram MarkdownV2.

Telegram MarkdownV2 rules:
  - Special chars that MUST be escaped outside formatting:
      _ * [ ] ( ) ~ ` > # + - = | { } . !
  - Bold:   *text*
  - Italic: _text_
  - Code (inline): `code`
  - Code block: ```lang\ncode\n```  (lang is optional but supported)
  - Strikethrough: ~text~
  - Underline: __text__
  - Spoiler: ||text||
  - Links: [text](url)

Strategy:
  We process the input line by line and block by block, distinguishing:
    1. Fenced code blocks (``` ... ```) — sent verbatim as code blocks
    2. Inline code (`...`) — sent as inline code
    3. Headings (#, ##, ###) — converted to bold
    4. Horizontal rules (---, ===) — replaced with a separator line
    5. Bold (**text** or __text__) — converted to *text*
    6. Italic (*text* or _text_) — converted to _text_
    7. Strikethrough (~~text~~) — converted to ~text~
    8. Unordered lists (-, *, +) — bullet kept, text escaped
    9. Ordered lists (1. 2.) — kept as-is, text escaped
    10. Blockquotes (> text) — converted to Telegram > blockquote
    11. Everything else — special chars escaped

The output is safe to send with parse_mode="MarkdownV2".
"""

import re

# Characters that must be escaped in MarkdownV2 outside of formatting constructs
_ESCAPE_CHARS = r"\_*[]()~`>#+-=|{}.!"

def _escape(text: str) -> str:
    """Escape all MarkdownV2 special characters in plain text."""
    # Use a simple char-by-char approach to avoid double-escaping
    result = []
    for ch in text:
        if ch in _ESCAPE_CHARS:
            result.append("\\" + ch)
        else:
            result.append(ch)
    return "".join(result)


def _escape_url(url: str) -> str:
    """Escape only ) and \\ inside a URL (inside parentheses in MarkdownV2 links)."""
    return url.replace("\\", "\\\\").replace(")", "\\)")


def _process_inline(text: str) -> str:
    """
    Convert inline markdown to MarkdownV2 within a single line of text.
    Order matters: process code first (to avoid escaping its contents),
    then links, then bold/italic/strikethrough, then escape remaining plain text.
    """
    # We'll build the result by scanning through segments
    # Segments can be: plain, inline-code, bold, italic, strikethrough, link
    result = []
    i = 0
    n = len(text)

    while i < n:
        # Inline code: `...`
        if text[i] == "`" and not text[i:].startswith("```"):
            end = text.find("`", i + 1)
            if end != -1:
                code_content = text[i+1:end]
                # Inside inline code, only escape ` and \
                safe = code_content.replace("\\", "\\\\").replace("`", "\\`")
                result.append(f"`{safe}`")
                i = end + 1
                continue

        # Markdown link: [label](url)
        if text[i] == "[":
            m = re.match(r"\[([^\]]*)\]\(([^)]*)\)", text[i:])
            if m:
                label = _process_inline(m.group(1))
                url   = _escape_url(m.group(2))
                result.append(f"[{label}]({url})")
                i += m.end()
                continue

        # Bold: **text** or __text__  (must check before italic)
        if text[i:i+2] in ("**", "__"):
            marker = text[i:i+2]
            end = text.find(marker, i + 2)
            if end != -1:
                inner = _process_inline(text[i+2:end])
                result.append(f"*{inner}*")
                i = end + 2
                continue

        # Italic: *text* or _text_   (single marker)
        if text[i] in ("*", "_"):
            marker = text[i]
            # Make sure it's not the start of ** or __
            if text[i:i+2] not in ("**", "__"):
                end = text.find(marker, i + 1)
                # Avoid matching across word boundaries for _
                if end != -1:
                    inner = _process_inline(text[i+1:end])
                    result.append(f"_{inner}_")
                    i = end + 1
                    continue

        # Strikethrough: ~~text~~
        if text[i:i+2] == "~~":
            end = text.find("~~", i + 2)
            if end != -1:
                inner = _process_inline(text[i+2:end])
                result.append(f"~{inner}~")
                i = end + 2
                continue

        # Plain character — escape it
        result.append(_escape(text[i]))
        i += 1

    return "".join(result)


def convert(text: str) -> str:
    """
    Convert LLM-generated Markdown text to Telegram MarkdownV2.
    Returns a string safe for send_message(..., parse_mode='MarkdownV2').
    """
    if not text:
        return ""

    lines = text.splitlines()
    output: list[str] = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]

        # ---- Fenced code block ````lang ... ``` ----
        fence_match = re.match(r"^(`{3,})([\w\-+#. ]*)$", line)
        if fence_match:
            fence = fence_match.group(1)
            lang  = fence_match.group(2).strip()
            i += 1
            code_lines = []
            while i < n and not lines[i].startswith(fence):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing fence
            code_body = "\n".join(code_lines)
            # Escape only ` and \ inside the code block
            safe_body = code_body.replace("\\", "\\\\").replace("`", "\\`")
            lang_tag  = lang if lang else ""
            output.append(f"```{lang_tag}\n{safe_body}\n```")
            continue

        # ---- Heading: # H1  ## H2  ### H3 ... ----
        heading_match = re.match(r"^(#{1,6})\s+(.*)", line)
        if heading_match:
            content = heading_match.group(2).strip()
            output.append(f"*{_process_inline(content)}*")
            i += 1
            continue

        # ---- Horizontal rule: ---, ***, === ----
        if re.match(r"^[-*_]{3,}\s*$", line):
            output.append(_escape("─" * 20))
            i += 1
            continue

        # ---- Blockquote: > text ----
        if line.startswith("> ") or line == ">":
            quote_lines = []
            while i < n and (lines[i].startswith("> ") or lines[i] == ">"):
                quote_lines.append(lines[i][2:] if lines[i].startswith("> ") else "")
                i += 1
            # Telegram MarkdownV2 blockquote: each line prefixed with >
            for ql in quote_lines:
                output.append(f">{_process_inline(ql)}")
            continue

        # ---- Unordered list: - item  * item  + item ----
        ul_match = re.match(r"^(\s*)([-*+])\s+(.*)", line)
        if ul_match:
            indent = ul_match.group(1)
            content = ul_match.group(3)
            # Use • as bullet (escaped dash causes issues)
            bullet_indent = "  " * (len(indent) // 2)
            output.append(f"{bullet_indent}• {_process_inline(content)}")
            i += 1
            continue

        # ---- Ordered list: 1. item ----
        ol_match = re.match(r"^(\s*)(\d+)[.)]\s+(.*)", line)
        if ol_match:
            indent  = ol_match.group(1)
            num     = ol_match.group(2)
            content = ol_match.group(3)
            bullet_indent = "  " * (len(indent) // 2)
            output.append(f"{bullet_indent}{_escape(num + '.')} {_process_inline(content)}")
            i += 1
            continue

        # ---- Empty line ----
        if line.strip() == "":
            output.append("")
            i += 1
            continue

        # ---- Normal paragraph line ----
        output.append(_process_inline(line))
        i += 1

    return "\n".join(output)
