#!/usr/bin/env python3
import asyncio
import os
import sys
import json
from contextlib import AsyncExitStack

# Add python path for mcp packages
sys.path.append("/opt/nemesis8/mcp-packages")

try:
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
except ImportError:
    # If not running in virtualenv, try using venv sys path
    sys.path.append("/opt/mcp-venv/lib/python3.10/site-packages")
    sys.path.append("/opt/mcp-venv/lib/python3.11/site-packages")
    sys.path.append("/opt/mcp-venv/lib/python3.12/site-packages")
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

HYPERIA_URL = os.environ.get("HYPERIA_URL")
if not HYPERIA_URL:
    if os.path.exists("/.dockerenv"):
        HYPERIA_URL = "http://host.docker.internal:9800"
    else:
        HYPERIA_URL = "http://localhost:9800"
HYPERIA_URL = HYPERIA_URL.rstrip("/")
MCP_URL = HYPERIA_URL + "/mcp"
HYPERIA_AGENT_TOKEN = os.environ.get("HYPERIA_AGENT_TOKEN", "").strip()
AUTH_HEADERS = {"Authorization": f"Bearer {HYPERIA_AGENT_TOKEN}"} if HYPERIA_AGENT_TOKEN else None

async def list_tools():
    try:
        async with AsyncExitStack() as stack:
            read, write, _ = await stack.enter_async_context(
                streamablehttp_client(MCP_URL, headers=AUTH_HEADERS)
            )
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.list_tools()
                tools_list = []
                for tool in result.tools:
                    tools_list.append({
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.inputSchema
                    })
                print(json.dumps(tools_list))
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)

async def call_tool(name, arguments_str):
    try:
        arguments = json.loads(arguments_str)
    except Exception as e:
        print(f"Error parsing arguments JSON: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        async with AsyncExitStack() as stack:
            read, write, _ = await stack.enter_async_context(
                streamablehttp_client(MCP_URL, headers=AUTH_HEADERS)
            )
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments)
                content_list = []
                for block in result.content:
                    if hasattr(block, 'text'):
                        content_list.append(block.text)
                    else:
                        content_list.append(str(block))
                print("\n".join(content_list))
    except Exception as e:
        print(f"Error calling tool: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 hyperia_helper.py [list|call] [tool_name] [arguments_json]")
        sys.exit(1)
    
    cmd = sys.argv[1]
    if cmd == "list":
        asyncio.run(list_tools())
    elif cmd == "call":
        if len(sys.argv) < 4:
            print("Usage: python3 hyperia_helper.py call [tool_name] [arguments_json]")
            sys.exit(1)
        asyncio.run(call_tool(sys.argv[2], sys.argv[3]))
