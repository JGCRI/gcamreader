# Structural comparison of GCAM output databases

**Result: structural differences found** in 2 of 2 candidates: 4 findings covering 2 added and 2 removed elements. Numeric values were not compared.

## Inputs

| # | Label | Database | Scenario | GCAM version | Elements | Fingerprint | Role |
|---|---|---|---|---|---|---|---|
| 1 | GCAM | `data/database_GCAM` | `GCAM@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `010101010101` | baseline |
| 2 | GCAM_units | `data/database_GCAM_units` | `GCAM_units@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `020202020202` |  |
| 3 | GCAM_restructured | `data/database_GCAM_restructured` | `GCAM_restructured@2026-1-6T10:39:43-07:00` | ver_9.0_rgcam-v9.0 | 18 | `030303030303` |  |

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

### 2: GCAM_units vs baseline 1: GCAM

| Kind | Findings | Elements |
|---|---|---|
| added | 0 | 0 |
| removed | 0 | 0 |
| attribute changed | 1 | 1 |
| count changed | 0 | 0 |
| metadata variants changed | 0 | 0 |
| **total** | **1** |  |

Descendant elements summarized under added/removed paths: 0. Warnings: 0.

### 3: GCAM_restructured vs baseline 1: GCAM

| Kind | Findings | Elements |
|---|---|---|
| added | 2 | 2 |
| removed | 1 | 2 |
| attribute changed | 0 | 0 |
| count changed | 0 | 0 |
| metadata variants changed | 0 | 0 |
| **total** | **3** |  |

Descendant elements summarized under added/removed paths: 1. Warnings: 0.

## Matrix

One row per differing path (and attribute); one column per input. A cell shows the value on that input: `present`/`absent`, an attribute value, a count, or the metadata variants. A candidate cell equal to the baseline means that comparison found no difference at that exact path; a difference in an ancestor is its own row.

| Path | Kind | Attribute | 1: GCAM | 2: GCAM_units | 3: GCAM_restructured |
|---|---|---|---|---|---|
| `scenario > world > region[name=China] > sector[name=new]` | added |  | absent | absent | present |
| `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2020..2025 (2 values)]` | removed |  | present | present | absent |
| `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2025]` | attribute changed | `unit` | EJ | PJ | EJ |
| `scenario > world > region[name=USA] > sector[name=transport] > subsector[name=rail]` | added |  | absent | absent | present |

## Findings: 2: GCAM_units vs baseline 1: GCAM

### region[name=USA]

| Kind | Tag | Findings | Elements | Descendants |
|---|---|---|---|---|
| attribute changed | `physical-output` | 1 | 1 | 0 |

- **attribute changed** `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2025]`: Attributes changed: unit changed ('EJ' -> 'PJ')

## Findings: 3: GCAM_restructured vs baseline 1: GCAM

### region[name=China]

| Kind | Tag | Findings | Elements | Descendants |
|---|---|---|---|---|
| added | `sector` | 1 | 1 | 1 |

- **added** `scenario > world > region[name=China] > sector[name=new]`: Added: 1 descendant element(s)

### region[name=USA]

| Kind | Tag | Findings | Elements | Descendants |
|---|---|---|---|---|
| added | `subsector` | 1 | 1 | 0 |
| removed | `physical-output` | 1 | 2 | 0 |

- **removed (2 elements)** `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2020..2025 (2 values)]`: Removed 2 physical-output elements (2 occurrence(s), 0 descendant element(s)): physical-output[vintage=2020], physical-output[vintage=2025]
- **added** `scenario > world > region[name=USA] > sector[name=transport] > subsector[name=rail]`: Added: 0 descendant element(s)

## Warnings and limitations

- No warnings were raised during extraction or comparison.
- Only structure and metadata are compared; numeric values are ignored. A present element whose value is `0` is still present.
- A renamed or moved element appears as a removal plus an addition; no rename detection is attempted.
- GCAM writes only non-zero results, and runs may cover different periods, so a path missing from one database can reflect output filtering or run coverage rather than a configuration change. Interpret added/removed leaves such as outputs, inputs and emissions with that in mind.
- Identity values are compared as text: `2020` and `2020.0` are different identities.
- Identical subtrees are recognised by a 128-bit additive fingerprint; an accidental collision hiding a difference is negligible for GCAM output.
- Descendant tag histograms are given only for added/removed subtrees with at least 50 descendants; descendant counts are always given.
- Remote databases are not supported; inputs must be local BaseX directories.
