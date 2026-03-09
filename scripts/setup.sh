#!/usr/bin/env bash
# setup.sh - Linux/macOS Setup Script
# Fuehrt einmalig aus: venv erstellen + alle Abhaengigkeiten installieren
# Aufruf: bash scripts/setup.sh  (aus dem Repository-Root)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="$REPO_ROOT/.venv"

echo "=== Masterthesis Analytics - Environment Setup ==="
echo "Repository: $REPO_ROOT"

# 1. Virtuelle Umgebung anlegen (falls noch nicht vorhanden)
if [ ! -d "$VENV_PATH" ]; then
    echo ""
    echo "[1/3] Erstelle virtuelle Umgebung..."
    python3 -m venv "$VENV_PATH"
else
    echo ""
    echo "[1/3] Virtuelle Umgebung bereits vorhanden, wird uebersprungen."
fi

# 2. Pip upgraden + Pakete installieren
echo ""
echo "[2/3] Installiere Abhaengigkeiten aus requirements.txt..."
"$VENV_PATH/bin/pip" install --upgrade pip --quiet
"$VENV_PATH/bin/pip" install -r "$REPO_ROOT/requirements.txt"

# 3. Sanity-Check: Imports testen
echo ""
echo "[3/3] Fuehre Import-Sanity-Check aus..."
"$VENV_PATH/bin/pytest" "$REPO_ROOT/tests/test_sanity.py" -v --no-header

echo ""
echo "=== Setup abgeschlossen ==="
echo "Umgebung aktivieren: source .venv/bin/activate"
