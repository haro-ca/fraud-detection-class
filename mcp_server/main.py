import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from mcp_server.server import server

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(server, host="0.0.0.0", port=8000)
