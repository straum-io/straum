---
name: task-closeout
description: Mandatory closing procedure for every Straum task that changed files. Runs validation, has the vision-guardian independently review the change and the proposed follow-ups, turns every recommendation into decision cards, and records them. Use before saying a task is done, before writing a completion report, and whenever the Stop hook reports that changes are not closed out.
---

# Task closeout

A task is not done when the code compiles. It is done when an independent
reviewer has confirmed it still serves the idea, and every open question has
become a decision the user can pick.

Run the steps in order. Do not skip one because the task was small; a small
task is where drift goes unnoticed.

## 1. Pin the inputs

Before anything else, write down for yourself:

* **TASK** — the user's request, verbatim, from the conversation. Not your
  paraphrase: drift starts with a paraphrase.
* **BASE** — the revision the task started from (`git merge-base HEAD @{u}`
  if there is an upstream, otherwise `HEAD`; if you committed during the
  task, the commit before your first one).

## 2. Validate

Run the validation from CLAUDE.md §17 for the crates the diff touches. A
failure is fixed now, not reported as a follow-up. If it cannot be fixed
inside the task's scope, stop and make it decision card D-<n> with
"unfinished" in the title.

## 3. Draft the report and the options

* The report, in the CLAUDE.md §17 format.
* Every recommendation, follow-up, trade-off or open question as a draft
  decision card, per the decision-options skill. Anything you did beyond
  TASK is reverted and turned into a card, unless it was required for TASK
  to work.

## 4. Independent review

Invoke the **vision-guardian** subagent. Pass exactly: TASK (verbatim),
BASE, the report, and the draft cards. Do not pass your reasoning or argue
your case in the prompt; the value of the review is that it did not follow
your reasoning.

Then act on its verdict:

| Verdict | Action |
|---|---|
| ALIGNED | continue |
| ALIGNED_WITH_NOTES | fix NOTE items that are in scope; list the rest in the report |
| DRIFT | fix what is inside TASK's scope; every remaining DRIFT becomes a card; re-run the guardian once |
| BLOCKED | fix every BLOCKER, re-validate, re-run the guardian; if a BLOCKER cannot be fixed, the task is reported as not done |

Apply OPTIONS VETTING: FIX items change the card, REJECT moves the option to
"Отсеяно" with the guardian's reason, MISSING options are worked out and
added. At most two guardian rounds; if the second is still DRIFT or BLOCKED,
report that plainly with its findings.

## 5. Record

Append the final cards to `docs/decisions/pending.md`. Nothing else goes
there.

## 6. Mark closed

Run `.claude/hooks/mark-closeout.sh` as the **last** command of the task.
It records a hash of the current change; the Stop hook uses it to know this
exact state was closed out. Any edit after it re-opens the task.

Do not commit unless the user asks. The user commits after he has seen the
closeout.

## 7. Report to the user

This is the end-of-task format of docs/architecture/unified-stream.md §7.

In Russian, in this order and nothing more:

````text
Итог: <one sentence — done / done with notes / not done>
Проверка видения: <verdict> — <one line; findings only if not ALIGNED>
Сделано: <from the report>
Проверено и чем: <tests, simulation scenarios, benchmarks — from the report>
Открытые риски: <what is not covered, what could still be wrong>
Решения: <the next step as 2–3 worked options — the cards, full text>
````

If there are no cards, write "Решения: нет открытых". Never end with a
question to the user other than the cards themselves.
