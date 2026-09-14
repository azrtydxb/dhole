/**
 * A step's secrets and capabilities, edited in the inspector (ADR 0028).
 *
 * The property under test is that every change is ONE operation of the
 * contract's own vocabulary — set_step_secret or set_step_capability — naming
 * one element, and that nothing is sent for an edit the control plane would
 * refuse anyway. The breaks these catch: a panel that sends the whole list
 * (a document-level write by another name), a rebind that sends an index (the
 * plane refuses it), a remove that forgets `remove`, and a malformed variable
 * name that reaches the wire.
 */
// @vitest-environment jsdom
import { create } from "@bufbuild/protobuf";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { Operation } from "../gen/dhole/v1/api_pb.js";
import { Capability } from "../gen/dhole/v1/common_pb.js";
import { StepSchema, StepSecretSchema } from "../gen/dhole/v1/pipeline_pb.js";
import { StepDeclarations } from "./StepDeclarations.js";

afterEach(cleanup);

function imageStep(capabilities: Capability[] = [Capability.NETWORK]) {
  return create(StepSchema, {
    id: "image",
    capabilities,
    secrets: [
      create(StepSecretSchema, {
        name: "registry-robot",
        env: "REGISTRY_USER",
      }),
    ],
  });
}

function mount(step = imageStep()) {
  const onOperation = vi.fn<(op: Operation) => void>();
  render(<StepDeclarations step={step} onOperation={onOperation} />);
  return onOperation;
}

/** sent is the single operation the panel emitted. */
function sent(
  onOperation: ReturnType<typeof mount>,
): Operation["kind"] | undefined {
  expect(onOperation).toHaveBeenCalledTimes(1);
  const op = onOperation.mock.calls[0]?.[0];
  return op?.kind;
}

describe("StepDeclarations", () => {
  it("shows the step's bindings and which capabilities it declares", () => {
    mount();
    expect(screen.getByTestId("secret-REGISTRY_USER").textContent).toContain(
      "REGISTRY_USER",
    );
    expect(
      screen.getByDisplayValue("registry-robot").getAttribute("data-testid"),
    ).toBe("secret-name-REGISTRY_USER");
    expect(
      screen
        .getByTestId("capability-CAPABILITY_NETWORK")
        .getAttribute("aria-pressed"),
    ).toBe("true");
    expect(
      screen
        .getByTestId("capability-CAPABILITY_PRIVILEGED")
        .getAttribute("aria-pressed"),
    ).toBe("false");
    // The zero value is not a capability anyone can declare.
    expect(screen.queryByTestId("capability-CAPABILITY_UNSPECIFIED")).toBe(
      null,
    );
  });

  it("binds a new secret as one set_step_secret that appends", () => {
    const onOperation = mount();
    fireEvent.change(screen.getByTestId("secret-new-env"), {
      target: { value: "NEXUS_PASSWORD" },
    });
    fireEvent.change(screen.getByTestId("secret-new-name"), {
      target: { value: "nexus-push" },
    });
    fireEvent.click(screen.getByTestId("secret-bind"));
    const kind = sent(onOperation);
    expect(kind?.case).toBe("setStepSecret");
    expect(kind?.value).toMatchObject({
      stepId: "image",
      env: "NEXUS_PASSWORD",
      name: "nexus-push",
      remove: false,
    });
    // No index: the plane appends, and the inverse it returns says where.
    expect(
      (kind?.value as { index?: number } | undefined)?.index,
    ).toBeUndefined();
  });

  it("rebinds an existing env without an index", () => {
    const onOperation = mount();
    const input = screen.getByTestId("secret-name-REGISTRY_USER");
    fireEvent.change(input, { target: { value: "nexus-user" } });
    fireEvent.keyDown(input, { key: "Enter" });
    const kind = sent(onOperation);
    expect(kind?.case).toBe("setStepSecret");
    expect(kind?.value).toMatchObject({
      env: "REGISTRY_USER",
      name: "nexus-user",
      remove: false,
    });
    expect(
      (kind?.value as { index?: number } | undefined)?.index,
    ).toBeUndefined();
  });

  it("unbinds a secret as a removal naming only its env", () => {
    const onOperation = mount();
    fireEvent.click(screen.getByTestId("secret-remove-REGISTRY_USER"));
    const kind = sent(onOperation);
    expect(kind?.case).toBe("setStepSecret");
    expect(kind?.value).toMatchObject({
      stepId: "image",
      env: "REGISTRY_USER",
      remove: true,
    });
  });

  it("refuses a name that is not an environment variable before sending", () => {
    const onOperation = mount();
    fireEvent.change(screen.getByTestId("secret-new-env"), {
      target: { value: "NEXUS-PASSWORD" },
    });
    fireEvent.change(screen.getByTestId("secret-new-name"), {
      target: { value: "nexus-push" },
    });
    fireEvent.click(screen.getByTestId("secret-bind"));
    expect(onOperation).not.toHaveBeenCalled();
    expect(screen.getByRole("alert").textContent).toContain(
      "not an environment variable name",
    );
  });

  it("refuses binding an env the step already binds, rather than rebinding it by surprise", () => {
    const onOperation = mount();
    fireEvent.change(screen.getByTestId("secret-new-env"), {
      target: { value: "REGISTRY_USER" },
    });
    fireEvent.change(screen.getByTestId("secret-new-name"), {
      target: { value: "other" },
    });
    fireEvent.click(screen.getByTestId("secret-bind"));
    expect(onOperation).not.toHaveBeenCalled();
    expect(screen.getByRole("alert").textContent).toContain("already bound");
  });

  it("declares and withdraws a capability one operation at a time", () => {
    const onOperation = mount();
    fireEvent.click(screen.getByTestId("capability-CAPABILITY_PRIVILEGED"));
    fireEvent.click(screen.getByTestId("capability-CAPABILITY_NETWORK"));
    expect(onOperation).toHaveBeenCalledTimes(2);
    expect(onOperation.mock.calls[0]?.[0].kind).toMatchObject({
      case: "setStepCapability",
      value: {
        stepId: "image",
        capability: Capability.PRIVILEGED,
        remove: false,
      },
    });
    expect(onOperation.mock.calls[1]?.[0].kind).toMatchObject({
      case: "setStepCapability",
      value: { stepId: "image", capability: Capability.NETWORK, remove: true },
    });
  });

  it("says when secrets are declared without CAPABILITY_SECRETS, and offers the one operation that fixes it", () => {
    const onOperation = mount(imageStep([Capability.NETWORK]));
    expect(
      screen.getByTestId("secrets-capability-missing").textContent,
    ).toContain("CAPABILITY_SECRETS");
    fireEvent.click(screen.getByTestId("secrets-capability-declare"));
    const kind = sent(onOperation);
    expect(kind).toMatchObject({
      case: "setStepCapability",
      value: { capability: Capability.SECRETS, remove: false },
    });
  });

  it("says nothing about the capability when the step declares it", () => {
    mount(imageStep([Capability.NETWORK, Capability.SECRETS]));
    expect(screen.queryByTestId("secrets-capability-missing")).toBe(null);
  });
});
