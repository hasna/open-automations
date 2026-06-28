import type { AutomationRuntimeBinding } from "../types.js";
import type { JsonObject } from "@hasna/actions";

export function createOpenLoopsRuntimeBinding(metadata: JsonObject = {}): AutomationRuntimeBinding {
  return {
    kind: "open-loops",
    name: "open-loops-runtime",
    description: "OpenLoops may claim and execute OpenAutomations queued actions through an explicit runtime binding.",
    handoff: "claim-queue",
    metadata,
  };
}

export function listDefaultRuntimeBindings(): AutomationRuntimeBinding[] {
  return [
    createOpenLoopsRuntimeBinding(),
  ];
}
