# Appendix: all findings and the full matrix

Companion to the main report. Nothing is truncated here: every finding of every comparison is listed by branch, followed by the complete matrix.

## Findings: 2: GCAM_units vs baseline 1: GCAM (1)

### region[name=USA] (1)

| Kind | Path | Elements | Descendants | Detail |
|---|---|---|---|---|
| attribute changed | `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2025]` | 1 | 0 | Attributes changed: unit changed ('EJ' -> 'PJ') |

## Findings: 3: GCAM_restructured vs baseline 1: GCAM (3)

### region[name=China] (1)

| Kind | Path | Elements | Descendants | Detail |
|---|---|---|---|---|
| added | `scenario > world > region[name=China] > sector[name=new]` | 1 | 1 | Added: 1 descendant element(s) |

### region[name=USA] (2)

| Kind | Path | Elements | Descendants | Detail |
|---|---|---|---|---|
| removed | `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2020..2025 (2 values)]` | 2 | 0 | Removed 2 physical-output elements (2 occurrence(s), 0 descendant element(s)): physical-output[vintage=2020], physical-output[vintage=2025] |
| added | `scenario > world > region[name=USA] > sector[name=transport] > subsector[name=rail]` | 1 | 0 | Added: 0 descendant element(s) |

## Matrix (4 rows)

| Path | Kind | Attribute | 1: GCAM | 2: GCAM_units | 3: GCAM_restructured |
|---|---|---|---|---|---|
| `scenario > world > region[name=China] > sector[name=new]` | added |  | absent | absent | present |
| `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2020..2025 (2 values)]` | removed |  | present | present | absent |
| `scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2025]` | attribute changed | `unit` | EJ | PJ | EJ |
| `scenario > world > region[name=USA] > sector[name=transport] > subsector[name=rail]` | added |  | absent | absent | present |
