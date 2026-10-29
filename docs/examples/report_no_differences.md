# Structural comparison of GCAM output databases

**Result: no structural differences** between the baseline and 1 candidate. Numeric values were not compared.

## Inputs

| # | Label | Database | Scenario | GCAM version | Elements | Fingerprint | Role |
|---|---|---|---|---|---|---|---|
| 1 | GCAM | `data/database_GCAM` | `GCAM@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `010101010101` | baseline |
| 2 | GCAM_copy | `data/database_GCAM_copy` | `GCAM_copy@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `020202020202` |  |

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

### 2: GCAM_copy vs baseline 1: GCAM

| Kind | Findings | Elements |
|---|---|---|
| added | 0 | 0 |
| removed | 0 | 0 |
| attribute changed | 0 | 0 |
| count changed | 0 | 0 |
| metadata variants changed | 0 | 0 |
| **total** | **0** |  |

Descendant elements summarized under added/removed paths: 0. Warnings: 0.

## Matrix

No differences, so the matrix is empty.

## Findings: 2: GCAM_copy vs baseline 1: GCAM

No structural differences.

## Warnings and limitations

- No warnings were raised during extraction or comparison.
- Only structure and metadata are compared; numeric values are ignored. A present element whose value is `0` is still present.
- A renamed or moved element appears as a removal plus an addition; no rename detection is attempted.
- GCAM writes only non-zero results, and runs may cover different periods, so a path missing from one database can reflect output filtering or run coverage rather than a configuration change. Interpret added/removed leaves such as outputs, inputs and emissions with that in mind.
- Identity values are compared as text: `2020` and `2020.0` are different identities.
- Identical subtrees are recognised by a 128-bit additive fingerprint; an accidental collision hiding a difference is negligible for GCAM output.
- Descendant tag histograms are given only for added/removed subtrees with at least 50 descendants; descendant counts are always given.
- Remote databases are not supported; inputs must be local BaseX directories.
