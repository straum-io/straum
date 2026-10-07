---
name: decision-options
description: Turns any recommendation, follow-up, open question or "next step" into decision cards — worked-out, vetted options the user can pick by id or discuss. Use whenever you are about to recommend, suggest, propose next steps, flag a trade-off, or ask the user to decide anything about Straum, at the end of a task and in the middle of one. Never hand the user an open question to analyse; hand them options you already analysed.
---

# Decision options

The user validates results; he does not do the analysis for you. Every
recommendation reaches him as a set of options that are already worked out,
already checked against `docs/architecture/unified-stream.md` and the ADRs
in `docs/adr/`, and executable the moment he picks
one.

Write decision cards **in English** (the user asked, 2026-10-05). Identifiers, file paths, code and
commands stay as they are.

## When a card is required

* Anything you would otherwise write as "recommend", "consider", "it may be
  worth", "next steps", "open question", "trade-off".
* A choice you cannot make yourself under CLAUDE.md §17 ("if behaviour is
  ambiguous ... stop and ask").
* Scope creep you noticed and did not do: it becomes a card, not a change.

Not required for: facts you are reporting, choices CLAUDE.md or the
constitution already decide, and trivia with one sensible answer — just do
those.

## Forbidden

These hand analysis back to the user. Do not write them, in any language:

````text
"стоит рассмотреть", "рекомендую проанализировать", "на ваше усмотрение",
"можно подумать о", "имеет смысл изучить", "нужно решить, что лучше",
"consider", "you may want to", "worth investigating", "up to you"
````

If you cannot choose between options because data is missing, the missing
data is itself the work: make one option "measure X first" with the exact
benchmark or experiment, its cost, and which result selects which option.

## Building the options

1. **State the decision** as one question with a concrete answer space.
2. **Generate candidates** — at least the plausible ones, including "do
   nothing / defer" when it is legitimate.
3. **Eliminate** candidates that violate `docs/architecture/unified-stream.md`
   (including its rule that no node changes shared state outside the log), an
   ADR, a contractual invariant, or a rejected decision in
   `docs/decisions/log.md`.
   They go to "Rejected" with the reason, one line each.
4. **Work out each survivor** fully (template below). An option the user
   would have to research before choosing is not finished.
5. **Recommend one** and say what would change the recommendation.
6. **Vet**: the vision-guardian reviews the options before the user sees them
   (task-closeout does this). Apply its FIX items; drop its REJECTs into
   "Rejected"; add its MISSING options.

Two to four options survive. More than four means the question is too broad
— split it into two decisions.

## Card template

Read `references/card-template.md` for the exact template. Every field is mandatory; write "none" rather than dropping a field.

## Numbering

Decisions are numbered `D-<n>`, globally and forever. Take the next number
after the highest one in `docs/decisions/log.md` and
`docs/decisions/pending.md`. Options are `A`, `B`, `C`, `D`. The user answers
with:

````text
D-12: B                 execute option B as written
D-12: B, but <change>   execute B with the amendment; restate it before starting
D-12: discuss           discuss; do not execute anything yet
D-12: reject            reject all options; record it
D-12: defer             keep it pending
````

When the user answers, remove the card from `pending.md`: append its full
text to `docs/decisions/archive/cards.md`, and add to `log.md` only the
compact entry (question, date, choice with the amendment, one line per
rejected option with its reason, a pointer to the archive). Then do the
work. The rejected-option lines are how the guardian knows not to let an
option come back without new evidence; the full card is read only when
needed.
