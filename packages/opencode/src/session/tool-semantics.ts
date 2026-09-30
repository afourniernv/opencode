import type { Tool } from "ai"

// Keep provenance out of the AI SDK tool schema and out of Relay dimensions.
// The resolved tool object itself is the capability token; the WeakSet does not
// retain tools after a request and never exposes the MCP server or tool name.
const dynamicMcpTools = new WeakSet<object>()

export function markDynamicMcpTool<T extends Tool>(tool: T): T {
  dynamicMcpTools.add(tool)
  return tool
}

export function toolSemanticCategory(tool: Tool | undefined): "mcp" | undefined {
  return tool && dynamicMcpTools.has(tool) ? "mcp" : undefined
}
