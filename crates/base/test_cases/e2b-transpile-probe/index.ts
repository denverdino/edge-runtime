Deno.serve(async () => {
  const results: Record<string, unknown> = {};

  results.stripped = await EdgeRuntime.transpile(
    "let x: number = 1; export {};",
    "probe.ts",
  );

  try {
    await EdgeRuntime.transpile("let x: = ;;;", "bad.ts");
    results.malformed = "no-error";
  } catch (error) {
    results.malformed = `threw: ${(error as Error).name}`;
  }

  try {
    await EdgeRuntime.transpile(
      "function unfinished(value: string) {",
      "truncated.ts",
    );
    results.truncated = "no-error";
  } catch (error) {
    results.truncated = `threw: ${(error as Error).name}`;
  }

  results.survived = await EdgeRuntime.transpile(
    "const alive: boolean = true;",
    "survived.ts",
  );

  // Deep nesting overflowed the parser's stack and aborted the whole process.
  // A stack overflow is not a panic, so catch_unwind cannot intercept it; the
  // defence is a large parser stack plus a source-size limit. Bracket-free
  // constructs bypass any bracket-counting guard, so cover those too.
  const deep: Record<string, string> = {};
  for (
    const [name, code] of [
      ["parens", "(".repeat(10_000) + "1" + ")".repeat(10_000)],
      ["unary", "!".repeat(50_000) + "1"],
      ["arrow", "x=>".repeat(50_000) + "1"],
      ["ifChain", "if(a)".repeat(50_000) + ";"],
    ] as const
  ) {
    try {
      await EdgeRuntime.transpile(code, `${name}.ts`);
      deep[name] = "no-error";
    } catch (e) {
      deep[name] = `threw: ${(e as Error).name}`;
    }
  }
  results.deeplyNested = deep;

  // A legitimate shallow nesting still works.
  results.shallowNested = await EdgeRuntime.transpile(
    "const a = ((1));",
    "ok.ts",
  );

  return Response.json(results);
});
