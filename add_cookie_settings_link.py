#!/usr/bin/env python3
"""
add_cookie_settings_link.py
---------------------------
Añade un enlace "Cookies" / "Cookie settings" justo al lado del enlace de
"Privacy Policy" en el pie de cada página. Al pulsarlo se llama a
openCookieSettings() (de cookie-consent.js) y vuelve a salir el banner.

Uso:
  python add_cookie_settings_link.py .            # SIMULACRO: solo informa
  python add_cookie_settings_link.py . --apply    # aplica los cambios

Es idempotente: si una página ya tiene el enlace, no la toca.
Si una página NO tiene enlace a privacy-policy, la salta y te lo avisa.
Con --apply guarda copia de los originales en <carpeta>_backup_cookielink/ (fuera del sitio).
"""
import argparse
import re
import shutil
import sys
from pathlib import Path

SKIP_DIRS = {".git", "node_modules", ".venv", "__pycache__"}
SCRIPT_RE = re.compile(r"<script\b[^>]*>.*?</script\s*>", re.I | re.S)
PRIV_RE = re.compile(
    r"<a\b(?P<attrs>[^>]*\bhref\s*=\s*[\"'][^\"']*privacy-policy[^\"']*[\"'][^>]*)>.*?</a\s*>", re.I | re.S
)
LAST_CLOSE_RE = re.compile(r"</(?:a|button)\s*>", re.I)
BLOCK_TAG_RE = re.compile(r"<\s*/?\s*(?:a|div|li|ul|p|br|button)\b", re.I)


def script_spans(text):
    return [(m.start(), m.end()) for m in SCRIPT_RE.finditer(text)]


def inside(spans, pos):
    return any(a <= pos < b for a, b in spans)


def find_privacy_link(text):
    spans = script_spans(text)
    found = [m for m in PRIV_RE.finditer(text) if not inside(spans, m.start())]
    return found[-1] if found else None  # el último suele ser el del pie de página


def new_anchor_from(attrs):
    """Copia el estilo (class/style) del enlace de Privacy Policy, pero con otro destino."""
    attrs = re.sub(r"\s(?:href|target|rel|id|onclick)\s*=\s*(\"[^\"]*\"|'[^']*')", "", " " + attrs, flags=re.I)
    return f'<a href="#" onclick="openCookieSettings();return false;"{attrs.rstrip()}>Cookie settings</a>'


def build_insertion(text, m):
    """Devuelve (texto_a_insertar, modo)."""
    start, end = m.start(), m.end()

    # Modo A: pie compacto de tus páginas principales (#infoFooter)
    pre = text[:start]
    i = pre.rfind('id="infoFooter"')
    if i != -1 and "</div>" not in pre[i:]:
        eol = "\r\n" if "\r\n" in text else "\n"
        return (f'{eol}    <span class="footer-sep">|</span>{eol}'
                f'    <button class="about-link" onclick="openCookieSettings()">Cookies</button>'), "pie compacto"

    # Modo B1: el enlace está dentro de una lista <li>…</li>
    li = re.search(r"(<li\b[^>]*>)\s*$", pre, re.I)
    after_li = re.match(r"\s*</li\s*>", text[end:], re.I)
    if li and after_li:
        eol = "\r\n" if "\r\n" in text else "\n"
        return f"</li>{eol}{li.group(1)}{new_anchor_from(m.group('attrs'))}", "lista <li>"

    # Modo B2: enlace suelto; copiamos el separador que ya usa la página
    window = pre[-300:]
    closes = list(LAST_CLOSE_RE.finditer(window))
    sep = " · "
    if closes:
        between = window[closes[-1].end():]
        visible = re.sub(r"<[^>]*>", "", between).strip()
        if not BLOCK_TAG_RE.search(between) and len(visible) <= 3:
            sep = between
    if not sep.strip() and not sep:
        sep = " "
    return sep + new_anchor_from(m.group("attrs")), "enlace suelto"


# Con un enlace más en el pie, en móviles estrechos (≈360 px) el pie compacto se desborda.
# Esta regla (solo para pantallas <=600 px) aprieta un poco el espaciado para que quepa todo.
FIT_CSS = ('<style id="cookie-footer-fit">@media (max-width:600px)'
           '{#infoFooter{gap:4px !important;padding:0 6px !important}}</style>')


def add_fit_css(text):
    if 'id="cookie-footer-fit"' in text:
        return text
    eol = "\r\n" if "\r\n" in text else "\n"
    m = re.search(r"</head\s*>", text, re.I)
    if m:
        return text[: m.start()] + FIT_CSS + eol + text[m.start():]
    i = text.find('id="infoFooter"')
    j = text.rfind("<div", 0, i)
    return text[:j] + FIT_CSS + eol + text[j:]


def process(text):
    if re.search(r"onclick=\"openCookieSettings\(", text):
        return None, "ya tiene el enlace"
    m = find_privacy_link(text)
    if not m:
        return None, "SIN enlace a privacy-policy"
    ins, mode = build_insertion(text, m)
    new = text[: m.end()] + ins + text[m.end():]
    if mode == "pie compacto":
        new = add_fit_css(new)
    return new, mode


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    if not root.is_dir():
        sys.exit(f"No existe la carpeta: {root}")
    backup = root.parent / f"{root.name}_backup_cookielink"

    files = sorted(p for p in root.rglob("*")
                   if p.suffix.lower() in (".html", ".htm") and not (set(p.relative_to(root).parts) & SKIP_DIRS))
    changed = already = missing = 0
    missing_list = []
    for p in files:
        rel = p.relative_to(root)
        try:
            original = p.read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            print(f"[ERROR] {rel}: no es UTF-8, saltada")
            continue
        new, info = process(original)
        if new is None:
            if info.startswith("SIN"):
                missing += 1
                missing_list.append(str(rel))
                print(f"[AVISO] {rel}: {info}")
            else:
                already += 1
                print(f"[ya ok] {rel}")
            continue
        changed += 1
        print(f"[{'CAMBIADA' if args.apply else 'cambiaría'}] {rel}: enlace añadido ({info})")
        if args.apply:
            dest = backup / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dest)
            p.write_bytes(new.encode("utf-8"))

    print(f"\n{len(files)} HTML · {changed} {'modificados' if args.apply else 'por modificar'}"
          f" · {already} ya ok · {missing} sin enlace a privacy-policy")
    if missing_list:
        print("Páginas sin enlace a privacy-policy (no se tocan):")
        for r in missing_list:
            print("   -", r)
    if args.apply and changed:
        print(f"Copia de los originales en: {backup}")
    if not args.apply:
        print("Esto fue un SIMULACRO. Añade --apply para aplicar los cambios.")


if __name__ == "__main__":
    main()
