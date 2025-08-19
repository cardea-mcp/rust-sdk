# How to Verify Rust Implementation with Official Python Implementation

## Verify Rust Client with Python Server

### SSE

Prepare the Python code
```bash
git clone https://github.com/openwallet-foundation-labs/mcp-over-tsp-python
```

Run the Python server with the SSE protocol
```bash
cd mcp-over-tsp-python/demo/server
uv run server.py sse
```

Run the Rust client with the SSE protocol in another shell session, appending the server DID
```bash
RUST_LOG=debug cargo run --example clients_sse_tmcp -- <server-did>
```

### HTTP

Terminate the MCP server and run the Python server with the HTTP protocol
```bash
cd mcp-over-tsp-python/demo/server
uv run server.py streamable-http
```

Run the Rust client with the HTTP protocol in another shell session, appending the server DID
```bash
RUST_LOG=debug cargo run --example clients_streamable_http_tmcp -- <server-did>
```

## Verify Rust Client with Rust Server

### SSE

Terminate the MCP server and run the Rust server with the SSE protocol
```bash
RUST_LOG=debug cargo run --example servers_counter_sse_tmcp
```

Run the Rust client with the SSE protocol in another shell session, appending the server DID
```bash
RUST_LOG=debug cargo run --example clients_sse_tmcp -- <server-did>
```


### HTTP

Terminate the MCP server and run the Rust server with the HTTP protocol
```bash
RUST_LOG=debug cargo run --example servers_counter_streamhttp_tmcp
```

Run the Rust client with the HTTP protocol in another shell session, appending the server DID
```bash
RUST_LOG=debug cargo run --example clients_streamable_http_tmcp -- <server-did>
```

or 

Terminate the MCP server and run the Rust server with the HTTP (use hyper version) protocol
```bash
RUST_LOG=debug cargo run --example counter_hyper_streamable_http_tmcp
```

Run the Rust client with the HTTP protocol in another shell session, appending the server DID
```bash
RUST_LOG=debug cargo run --example clients_streamable_http_tmcp -- <server-did>
```

## Verify Python Client with Rust Server

### SSE

Terminate the MCP server and run the Rust server with the SSE protocol
```bash
RUST_LOG=debug cargo run --example servers_counter_sse_tmcp
```

Run the Python client with the SSE protocol in another shell session, appending the server DID
```bash
cd mcp-over-tsp-python/demo/client
uv run client.py <server-did>
```