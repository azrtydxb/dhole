/**
 * The ways out of a modal.
 *
 * A modal covers the editor, so if Escape, the close button and the backdrop
 * all fail to fire, the only remaining exit is a page reload — and the break
 * is silent, because the panel still looks correct. These assert each exit
 * separately so a regression names which one went.
 *
 * The Tab test guards the other half: focus that escapes the panel lands on
 * controls behind a backdrop that swallows the clicks, so a keyboard user
 * appears to be typing into a dead UI.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Modal, ModalButton } from "./Modal.js";
import { EnginesModal, type FleetEngine } from "./EnginesModal.js";
import { RegistryModal } from "./RegistryModal.js";
import { SettingsModal } from "./SettingsModal.js";

afterEach(cleanup);

describe("Modal", () => {
  it("closes on Escape", () => {
    const onClose = vi.fn();
    render(
      <Modal title="settings" width={400} onClose={onClose}>
        <p>body</p>
      </Modal>,
    );
    fireEvent.keyDown(document, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("closes on the ✕ button", () => {
    const onClose = vi.fn();
    render(
      <Modal title="settings" width={400} onClose={onClose}>
        <p>body</p>
      </Modal>,
    );
    fireEvent.click(screen.getByLabelText("close"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("closes on the backdrop but not on the panel", () => {
    const onClose = vi.fn();
    const { container } = render(
      <Modal title="settings" width={400} onClose={onClose}>
        <p>body</p>
      </Modal>,
    );
    fireEvent.mouseDown(screen.getByRole("dialog"));
    expect(onClose).not.toHaveBeenCalled();
    const backdrop = container.firstElementChild;
    expect(backdrop).not.toBeNull();
    fireEvent.mouseDown(backdrop as Element);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("moves focus into the panel and keeps Tab inside it", () => {
    render(
      <Modal
        title="settings"
        width={400}
        onClose={vi.fn()}
        footer={
          <ModalButton kind="primary" onClick={vi.fn()}>
            done
          </ModalButton>
        }
      >
        <p>body</p>
      </Modal>,
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog.contains(document.activeElement)).toBe(true);

    const done = screen.getByText("done");
    done.focus();
    fireEvent.keyDown(document, { key: "Tab" });
    expect(dialog.contains(document.activeElement)).toBe(true);
    expect(document.activeElement).toBe(screen.getByLabelText("close"));
  });

  it("marks a panel with nothing behind it as sample", () => {
    render(
      <Modal title="plugin registry" width={400} sample onClose={vi.fn()}>
        <p>body</p>
      </Modal>,
    );
    expect(screen.getByText("sample · no endpoint")).not.toBeNull();
  });
});

describe("the panels that have no endpoint", () => {
  it("say so by default", () => {
    render(
      <SettingsModal tenant="acme" onClose={vi.fn()} onRevokeToken={vi.fn()} />,
    );
    expect(screen.getByText("sample · no endpoint")).not.toBeNull();
    cleanup();

    render(<RegistryModal onClose={vi.fn()} onInstall={vi.fn()} />);
    expect(screen.getByText("sample · no endpoint")).not.toBeNull();
  });
});

describe("EnginesModal", () => {
  const fleet: readonly FleetEngine[] = [
    {
      id: "containerd@gpu-01",
      state: "ready",
      capabilities: ["oci", "gpu"],
      os: "linux",
      arch: "amd64",
      slots: 4,
      protocolVersions: [1, 2],
      inFlight: 2,
    },
    {
      id: "k8s@prod",
      state: "offline",
      capabilities: ["k8s"],
      os: "linux",
      arch: "arm64",
      slots: 8,
      protocolVersions: [1],
      inFlight: 0,
      lostFor: "4m",
    },
  ];

  it("renders the fleet it was given and carries no sample marker", () => {
    render(
      <EnginesModal
        engines={fleet}
        onDrain={vi.fn()}
        onClose={vi.fn()}
        busUrl="nats://bus.acme.dev:4222"
      />,
    );
    expect(screen.getByText("containerd@gpu-01")).not.toBeNull();
    expect(screen.getByText("2 steps running")).not.toBeNull();
    expect(screen.getByText("heartbeat lost 4m ago")).not.toBeNull();
    expect(screen.queryByText("sample · no endpoint")).toBeNull();
  });

  it("offers drain only for an engine that is taking work", () => {
    const onDrain = vi.fn();
    render(
      <EnginesModal
        engines={fleet}
        onDrain={onDrain}
        onClose={vi.fn()}
        busUrl="nats://bus.acme.dev:4222"
      />,
    );
    const buttons = screen.getAllByText("drain");
    expect(buttons).toHaveLength(1);
    fireEvent.click(buttons[0] as Element);
    expect(onDrain).toHaveBeenCalledWith("containerd@gpu-01");
  });

  it("says the fleet is empty rather than showing nothing", () => {
    render(
      <EnginesModal
        engines={[]}
        onDrain={vi.fn()}
        onClose={vi.fn()}
        busUrl="nats://bus.acme.dev:4222"
      />,
    );
    expect(screen.getByText(/no engines registered/)).not.toBeNull();
  });
});
