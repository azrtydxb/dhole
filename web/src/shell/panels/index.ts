/**
 * The panels, in one import.
 *
 * The editor shell mounts all of these and none of them know about each other;
 * a barrel keeps that true, because the alternative — the shell reaching into
 * seven paths — is what tempts a panel to import a sibling for "just one
 * type" and turn a pure component into part of a graph.
 */
export { Modal, ModalButton, useDialogKeys } from "./Modal.js";
export { SettingsModal, type SettingsTab } from "./SettingsModal.js";
export { EnginesModal, type FleetEngine } from "./EnginesModal.js";
export {
  RegistryModal,
  sampleRegistryPlugins,
  type RegistryPlugin,
} from "./RegistryModal.js";
export { GitModal, type GitMirror } from "./GitModal.js";
export { ImportModal, type ImportSource } from "./ImportModal.js";
export { CommandPalette, matches, type Command } from "./CommandPalette.js";
export { Toasts, type Toast, type ToastTone } from "./Toasts.js";
