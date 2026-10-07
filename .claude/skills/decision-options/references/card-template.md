# Decision card template

Every field is mandatory; write "none" rather than dropping one. Facts,
measurements and hypotheses are labelled as such: a hypothesis is never
presented as a result.

````markdown
## D-<n>. <the decision as one question>

**Context.** 2–3 sentences of fact: what was found, where (file:line), how it
was confirmed.
**Why now.** What is blocked or gets worse if it waits.
**Vision.** The sections of docs/architecture/unified-stream.md and the ADRs
the decision serves.

### A. <short name> — recommended
- **What we do:** concrete changes: crates, files, types, tests.
- **Result:** what is true afterwards, in a checkable form.
- **Cost:** S / M / L and what it includes.
- **Risks:** what can go wrong → how it is caught.
- **Invariants:** which are touched and why they hold (or: none).
- **Reversibility:** easy / changes a format or API / irreversible.
- **Check:** commands, tests, simulation scenarios, benchmarks.
- **Fact / hypothesis:** what is measured, what is assumed.

### B. <…>
(same fields)

### C. Do nothing / defer
- **When this is right:** …
- **Cost of waiting:** …

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | | | |
| Risk to invariants | | | |
| Cost | | | |
| Reversibility | | | |

**Recommendation.** A, because … B instead if … (a concrete condition).
**What would change it.** The fact or measurement that would switch the choice.
**Rejected.**
- <option> — violates <unified-stream §, ADR, invariant or D-k>.

**Answer:** `D-<n>: A` · `D-<n>: B, but …` · `D-<n>: discuss` · `D-<n>: defer`
````
