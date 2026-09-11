/**
 * The three surfaces where an agent's work meets a person's judgement.
 *
 * They are grouped because they are one story told in order: the assistant
 * PROPOSES operations, DiffReview is where a person reads them before they
 * land, and ApprovalGate is where a person releases a paused run. Only the
 * first is sample data; the other two run on contract the plane already has.
 */
export { AssistantPanel } from "./AssistantPanel.js";
export type { AssistantMessage, AssistantOp } from "./AssistantPanel.js";
export { DiffReview } from "./DiffReview.js";
export type { ReviewChange, ReviewChangeKind } from "./DiffReview.js";
export { ApprovalGate } from "./ApprovalGate.js";
export type { GateRun, GateStep } from "./ApprovalGate.js";
