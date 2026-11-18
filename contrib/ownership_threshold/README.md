# ownership_threshold

The `sanction.control` topic marks companies, assets and vessels owned by a
sanctioned party, directly or through subsidiaries. As the
[announcement](https://www.opensanctions.org/articles/2026-08-13-sanction-control/)
says, it applies no percentage threshold: it records that the relationship
exists, and the threshold depends on the context of the review.

For a US review that threshold is usually the OFAC 50 Percent Rule (entities
owned 50% or more, in the aggregate, by blocked persons) or the BIS Affiliates
Rule. This script applies both to the data a yente instance already indexes.
It reads the same `entities.ftm.json` export, walks its `Ownership` links with
their percentages and dates, and writes a verdict and the chain of owners
behind it for every entity a rule reaches. The rules come from
[ownership-rules](https://github.com/Mattbusel/ownership-rules) (Apache-2.0,
standard library only), which also ships the OFAC and BIS FAQ examples as test
cases.

Nothing in yente changes. The output is a JSON lines file keyed by entity ID,
so a hit from `/match` can be looked up in it during review. Results are
evidence for a compliance review, not a legal determination.

## Running

```bash
pip install "ownership-rules @ https://github.com/Mattbusel/ownership-rules/releases/download/v0.2.1/ownership_rules-0.2.1-py3-none-any.whl"
wget https://data.opensanctions.org/datasets/latest/default/entities.ftm.json
python contrib/ownership_threshold/ownership_threshold.py entities.ftm.json --out verdicts.jsonl
```

Options:

- `--rule`: `ofac-50`, `bis-affiliates` (both by default), or the path of a
  rule file in the ownership-rules format. Repeat it for several rules.
- `--as-of YYYY-MM-DD`: evaluate on that day. Holdings with an `endDate` on or
  before it no longer count. The default is today.
- `--directorships`: count a sanctioned director as control. Control without
  ownership is reported as `control_only`, not as covered (OFAC FAQ 398).
- Several files can be given, for example the OpenSanctions export plus your
  own ownership data in FtM, as long as they use the same entity IDs.

Each output line looks like this (the proof is shortened here):

```json
{"entity": "NK-5TwSPxC8EPGNBDUevDnP9K", "verdict": "definitely", "share": [100, 100],
 "seeds": ["NK-Mh9z4NDaqKNrLmcdyAoQnR", "NK-cwd2vrpPxdd9Sxm8uAFqTF"], "rule": "ofac-50",
 "caption": "Mikhailovsky HBI", "topics": ["sanction.linked", "sanction.control"],
 "explain": "... NK-5TwSPxC8EPGNBDUevDnP9K: definitely (covered share 100-100%) because NK-Mh9z4NDaqKNrLmcdyAoQnR (listed) owns 55% of NK-5TwSPxC8EPGNBDUevDnP9K; NK-cwd2vrpPxdd9Sxm8uAFqTF (listed) owns 45% of NK-5TwSPxC8EPGNBDUevDnP9K"}
```

`verdict` is `definitely` when known shares held by covered parties reach the
threshold, `possibly` when only the upper end of a share range does or a
covered party holds a share of unknown size, and `control_only` for control
without ownership. Entities a rule does not reach are left out.

## On the `default` export of 2026-11-18

Run in WSL (Ubuntu, Python 3.12) on the 2.6 GB `default` export: about 60 s and
2.5 GB of memory. The script reads the file twice, the second time keeping only
the entities that the ownership and sanction links name.

- 208,789 ownership edges, 29,590 entities tagged `sanction.control`.
- OFAC 50 Percent Rule: 3,656 entities `definitely` and 4,500 `possibly`.
  Of the 29,590 `sanction.control` entities, 3,447 are `definitely`, 4,086
  `possibly`, and 22,057 are not reached (the owners' known shares stay below
  50%, or the owners are sanctioned by lists other than the SDN List).
- 209 of the `definitely` entities are not tagged `sanction.control`. All 209
  are tagged `sanction`: they are listed elsewhere, but not on the SDN List, and
  SDN owners hold 50% or more of them.
- BIS Affiliates Rule: 3,923 `definitely` and 5,692 `possibly`.

The SDN List's own ownership links ("Owned or Controlled By") carry no
percentage, so on their own they give `possibly`. The percentages come mostly
from the company registry data in the export, such as `ext_ru_egrul`.

## Seeds

Only US lists start the rules. The OFAC rule starts from entities in
`us_ofac_sdn`. The BIS rule starts from Entity List parties (sanctions with
programId `US-BIS-EL`), the MEU List (`us_bis_mieu`) and SDNs under the
programs the rule names (Russia, Belarus, Ukraine-related, terrorism,
proliferation and narcotics). Other lists can be mapped with
`ownership_rules.formats.from_ftm(lists=...)` and a rule file.
