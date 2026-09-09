import { expect, test } from "@playwright/test";

// A smoke test only: it proves the app boots against a live `dhole serve`
// fixture. The interaction tests arrive with the canvas.
test("the app shell loads", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Dhole" })).toBeVisible();
});
