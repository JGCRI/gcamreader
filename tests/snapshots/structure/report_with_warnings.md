# Structural comparison of GCAM output databases

**Result: structural differences found** in 1 of 1 candidate: 2 findings covering 0 added and 0 removed elements. Numeric values were not compared.

## Inputs

| # | Label | Database | Scenario | GCAM version | Elements | Fingerprint | Role |
|---|---|---|---|---|---|---|---|
| 1 | dup | `data/database_dup` | `dup@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `010101010101` | baseline |
| 2 | single | `data/database_single` | `single@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `020202020202` |  |

Each database contributes exactly one scenario run (`name@date`); runs are never merged. The fingerprint covers the BaseX metadata file and the names and sizes of the storage files.

## Settings

- gcamreader version: `1.5.0-test`
- Identity attributes: name, year, vintage, ageGroup
- Excluded attributes: /scenario/@name, /scenario/@date
- Excluded elements: none
- Ignored attributes: none
- Fallback: every other attribute is metadata compared for presence and value; unfamiliar elements are traversed like any other; element text (numeric values) is never compared.

## Counting definitions

- **Path**: `tag[identity, ...]` segments joined by ` > `, from the `scenario` document element down. Identity attributes are those listed in the settings; all other attributes are metadata.
- **Count / occurrences**: how many sibling elements share a path and the same metadata. Children of repeated siblings are merged, so their counts are totals across the repeats.
- **Added / removed**: a path present in one input only. Nothing below it is listed separately; **descendants** gives the number of elements in that subtree and, for subtrees with at least 50 descendants, a histogram of their tags.
- **Grouped finding**: added or removed siblings with the same parent and tag are reported together; **elements** is the number of members and the detail lists every member identity. Paths of grouped findings end in a summary such as `Non-CO2[year=2040..2100 (13 values)]`.
- **Attribute changed**: same path on both sides, one metadata variant each, with different attribute values (added, removed or changed).
- **Count changed**: same path and metadata on both sides, different number of occurrences.
- **Metadata variants changed**: a path whose occurrences carry several metadata variants on at least one side; all variants are listed, nothing is collapsed.
- Element text (numeric values) is never compared.

## Summary

### 2: single vs baseline 1: dup

| Kind | Findings | Elements |
|---|---|---|
| added | 0 | 0 |
| removed | 0 | 0 |
| attribute changed | 0 | 0 |
| count changed | 2 | 2 |
| metadata variants changed | 0 | 0 |
| **total** | **2** |  |

Descendant elements summarized under added/removed paths: 0. Warnings: 1.

## Matrix

One row per differing path (and attribute); one column per input. A cell shows the value on that input: `present`/`absent`, an attribute value, a count, or the metadata variants. A candidate cell equal to the baseline means that comparison found no difference at that exact path; a difference in an ancestor is its own row.

| Path | Kind | Attribute | 1: dup | 2: single |
|---|---|---|---|---|
| `r > s[name=a]` | count changed |  | 2 | 1 |
| `r > s[name=a] > t` | count changed |  | 2 | 1 |

## Findings: 2: single vs baseline 1: dup

### r

| Kind | Tag | Findings | Elements | Descendants |
|---|---|---|---|---|
| count changed | `s` | 1 | 1 | 0 |
| count changed | `t` | 1 | 1 | 0 |

- **count changed** `r > s[name=a]`: Count changed: 2 in baseline, 1 in candidate
- **count changed** `r > s[name=a] > t`: Count changed: 2 in baseline, 1 in candidate

## Warnings and limitations

- Warning: Java 21.0.1 is newer than the tested versions
- Warning: dup: 1 repeated sibling element(s) with children at /r/s: their subtrees are merged, so counts below are totals across the repeats
- Only structure and metadata are compared; numeric values are ignored. A present element whose value is `0` is still present.
- A renamed or moved element appears as a removal plus an addition; no rename detection is attempted.
- GCAM writes only non-zero results, and runs may cover different periods, so a path missing from one database can reflect output filtering or run coverage rather than a configuration change. Interpret added/removed leaves such as outputs, inputs and emissions with that in mind.
- Identity values are compared as text: `2020` and `2020.0` are different identities.
- Identical subtrees are recognised by a 128-bit additive fingerprint; an accidental collision hiding a difference is negligible for GCAM output.
- Descendant tag histograms are given only for added/removed subtrees with at least 50 descendants; descendant counts are always given.
- Remote databases are not supported; inputs must be local BaseX directories.
