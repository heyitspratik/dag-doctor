# How confidence is computed

The confidence on a diagnosis is computed in code from the shape of the investigation. It
is never the model's self-reported number.

This is not a stylistic preference. A language model asked how confident it is will answer
somewhere around ninety percent whether it is right or wrong, and it will say it in the
same tone either way. A number like that is worse than no number, because it looks like
information. The score below is not perfectly calibrated either, but it is at least a
function of things that actually happened.

## The formula

```
confidence = base
           + signature_match
           + hypothesis_test
           + evidence_support
           - refutation_penalty
```

clamped to the range 0 to 1. The implementation is
[`graph/confidence.py`](../src/dag_doctor/graph/confidence.py).

| Term | Range | What it responds to |
|---|---|---|
| `base` | 0.15 | Fixed. Having run an investigation is not itself evidence. |
| `signature_match` | 0 to 0.20 | Whether the exception matches a failure pattern the code already recognises, scaled by how decisive that pattern is. |
| `hypothesis_test` | 0 to 0.30 | Full marks if a hypothesis was tested and confirmed, a fifth if its test decided nothing, zero if it was never tested or was refuted. |
| `evidence_support` | 0 to 0.35 | How many **distinct tools** produced usable evidence, saturating at four. |
| `refutation_penalty` | 0 to -0.15 | 0.05 per hypothesis discarded before this one, capped. |

## Why these terms

**Distinct tools, not evidence items.** Calling one tool three times is a single line of
argument repeated. Counting each call separately would let a confident-sounding
investigation manufacture its own support by asking the same question again.

**A failed tool call supports nothing.** It is still recorded as evidence, because knowing
the metadata database was unreachable matters, but it cannot raise confidence.

**Refutations cost something.** A cause found on the fourth attempt deserves less
confidence than one found on the first, because the three before it were also plausible
and were also believed at the time.

**Saturation at four tools.** Four independent tools agreeing is a strong result. A fifth
is usually the same story retold, and without a ceiling a verbose investigation would
outscore a decisive one.

## Where the number is used

Confidence gates two decisions, both in [`graph/routing.py`](../src/dag_doctor/graph/routing.py):

- **Triage shortcut.** Skipping the investigation entirely requires the triage confidence
  to clear `TRIAGE_SHORTCUT_CONFIDENCE` (0.9 by default) **and** the model to have said no
  further evidence would help. Triage confidence itself requires two independent things to
  agree: a recognised failure pattern, and a model that classifies it the same way. Either
  alone is not enough, because a confident wrong answer is the worst outcome available.
- **Concluding.** A confirmed hypothesis still escalates if the total sits below
  `MIN_CONCLUDE_CONFIDENCE` (0.5 by default). Confirmation by one weak test is not a
  diagnosis.

Both thresholds are settings rather than constants, and a validator refuses a
configuration where the triage shortcut is easier to clear than a tested conclusion.

## Reading the score

Every diagnosis records its breakdown in the step trace, so the number can be argued with:

```
0.82 = 0.15 base + 0.20 signature + 0.30 test + 0.26 evidence +0.00 refutations
```

## Limitations, stated plainly

The weights are chosen by judgement, not fitted to data. There is no calibration set
saying that diagnoses scoring 0.8 are correct eighty percent of the time, and until the
feedback endpoint has collected real verdicts there cannot be one. Treat the score as an
ordering over diagnoses rather than a probability. The honest claim is that it responds to
the right things, not that it is right.
