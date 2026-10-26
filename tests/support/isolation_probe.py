from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import sys


workspace = Path("/workspace")
artifact = workspace / "probe-artifact.txt"
artifact.write_text("isolated-artifact\n", encoding="utf-8")

host_paths_hidden = all(not Path(path).exists() for path in sys.argv[1:])
secrets_absent = all(
    name not in os.environ
    for name in ("AGENTBUS_TOKEN", "OPENAI_API_KEY", "CODEX_API_KEY", "PALACE_SECRET")
)

network_blocked = False
try:
    with socket.create_connection(("1.1.1.1", 53), timeout=0.25):
        pass
except OSError:
    network_blocked = True

print(json.dumps({
    "artifact_written": artifact.read_text(encoding="utf-8") == "isolated-artifact\n",
    "host_paths_hidden": host_paths_hidden,
    "network_blocked": network_blocked,
    "secrets_absent": secrets_absent,
}, sort_keys=True))
