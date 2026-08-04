# Implementation Standards

These rules apply to the entire repository.

## Source structure

- Do not keep adding unrelated responsibilities to an already large source file. Split cohesive responsibilities into clearly named modules and source files when that makes the code easier to review, test, or maintain.
- Do not keep a method large when it contains multiple stages or decisions. Extract small, clearly named methods with one responsibility, and keep the caller focused on orchestration.
- Choose module and method boundaries by responsibility and invariants, not by arbitrary line-count targets. Avoid both monolithic files and mechanical fragmentation.
- Preserve public APIs, visibility, ownership, ordering, error semantics, and safety invariants when splitting existing code.
- Keep structural refactors within the approved feature scope. Do not introduce future-project capabilities as part of a cleanup.
- Verify refactors with focused tests for the affected behavior and the relevant regression suite. A structural change is not complete until behavior-equivalence evidence passes.
