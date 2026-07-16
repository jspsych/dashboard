# jsPsych Sustainability Report

Generated: 2026-07-16

Rolling 6-month windows (182 days) stepped quarterly across project history. Each table shows the most recent 8 windows.

## Summary

| Goal | Headline metric | Current | Previous (6 mo ago) | Change |
| --- | --- | --- | --- | --- |
| Goal 1: Contributors with merge access | `value` | 3 | 3 | +0.0% |
| Goal 2: Community PR review & merge time | `community_median_days` | 0.80 | 5.70 | -86.0% |
| Goal 3: Support responsiveness & participation | `median_first_response_days` | 0.39 | 0.16 | +141.9% |
| Goal 4: Community-repository contributions | `contributions` | 98 | 158 | -38.0% |
| Goal 5: New-contributor growth (core packages) | `new_contributors` | 8 | 15 | -46.7% |

## Goal 1: Contributors with merge access

_Increase the number of contributors with merge access._

**How this is measured.** The number of people who hold merge (write) access to the jsPsych repository as of the end of the window, taken from the maintained core-team roster in config/team.json rather than inferred from activity. A person is counted for a window if their merge access began on or before the window end and had not been revoked by then. This is a point-in-time headcount, not an activity measure: a core member is counted even if they made no contribution during the window.

Headline metric (`value`): Current window: **3** vs. 6 months earlier: **3** (change: +0.0%)

| window_end | value |
| --- | --- |
| 2024-10-01 | 3 |
| 2025-01-01 | 3 |
| 2025-04-01 | 3 |
| 2025-07-01 | 3 |
| 2025-10-01 | 3 |
| 2026-01-01 | 3 |
| 2026-04-01 | 3 |
| 2026-07-01 | 3 |

## Goal 2: Community PR review & merge time

_Reduce the time for code review and PR merges of community contributions._

**How this is measured.** The median time, in days, from opening to merging a pull request authored by a community member (anyone NOT on the core-team roster), over pull requests merged within the trailing six-month window on the main jsPsych repository. Only merged PRs count toward the median; still-open or closed-unmerged PRs are excluded, as are PRs authored by core-team members (tracked separately) and by bots. Lower is better.

Headline metric (`community_median_days`): Current window: **0.8** vs. 6 months earlier: **5.7** (change: -86.0%)

| window_end | community_median_days | core_median_days | community_merge_rate |
| --- | --- | --- | --- |
| 2024-10-01 | 2.13 | 0.37 | 65.38 |
| 2025-01-01 | 2.13 | 0.60 | 76.67 |
| 2025-04-01 | 4.27 | 9.94 | 81.82 |
| 2025-07-01 | 19.86 | 4.97 | 61.90 |
| 2025-10-01 | 10.09 | — | 50 |
| 2026-01-01 | 5.70 | 5.89 | 52.94 |
| 2026-04-01 | 5.08 | 5.89 | 77.78 |
| 2026-07-01 | 0.80 | 0.01 | 31.25 |

## Goal 3: Support responsiveness & participation

_Decrease the time for support responses and broaden participation._

**How this is measured.** The median time, in days, from opening a discussion to its first reply by someone other than the author, over discussions opened within the trailing six-month window on the main jsPsych repository. Replies by the discussion author (self-replies) and by bots do not count as a response; a discussion is looked at across all later comments so an answer arriving after the window still counts. Discussions that never received a qualifying reply are excluded from the median. Lower is better.

Headline metric (`median_first_response_days`): Current window: **0.39** vs. 6 months earlier: **0.16** (change: +141.9%)

| window_end | median_first_response_days | qa_answer_rate | core_response_share_pct |
| --- | --- | --- | --- |
| 2024-10-01 | 0.46 | 26.47 | 13.97 |
| 2025-01-01 | 0.72 | 12.12 | 20.97 |
| 2025-04-01 | 5.65 | 12.12 | 13.79 |
| 2025-07-01 | 3.25 | 13.64 | 8.33 |
| 2025-10-01 | 1.42 | 22.22 | 11.63 |
| 2026-01-01 | 0.16 | 20 | 20 |
| 2026-04-01 | 0.54 | 30.77 | 8.70 |
| 2026-07-01 | 0.39 | 25 | 10 |

## Goal 4: Community-repository contributions

_Increase the rate of contribution to community repositories._

**How this is measured.** The total number of contributions -- pull requests, issues, comments, and reviews -- made within the trailing six-month window across the community repositories (jspsych-contrib and jspsych-timelines). Bot-authored activity is excluded via the central bot filter. This is a volume measure of activity flowing into the wider jsPsych ecosystem; the companion unique-contributor count deduplicates people who are active in more than one community repository. Higher is better.

Headline metric (`contributions`): Current window: **98** vs. 6 months earlier: **158** (change: -38.0%)

| window_end | contributions | unique_contributors |
| --- | --- | --- |
| 2024-10-01 | 106 | 16 |
| 2025-01-01 | 132 | 20 |
| 2025-04-01 | 128 | 18 |
| 2025-07-01 | 160 | 17 |
| 2025-10-01 | 198 | 15 |
| 2026-01-01 | 158 | 12 |
| 2026-04-01 | 74 | 14 |
| 2026-07-01 | 98 | 18 |

## Goal 5: New-contributor growth (core packages)

_Accelerate new-contributor growth on core packages._

**How this is measured.** The headline figure is engagement-based: the number of people whose FIRST-EVER contribution of any kind to the main jsPsych repository -- authoring a pull request, opening an issue, leaving a comment, or submitting a review -- falls within the trailing six-month window. Each person is attributed to the window of their first-ever contribution across all history, so an established contributor active again in a later window is not re-counted as new; bot accounts are excluded. A second, commit-based figure is reported alongside it: the number of distinct GitHub accounts whose first non-merge commit on the default branch falls within the window. The commit-based figure matches GitHub's own contributor graph (merge commits and commits whose email is not linked to a GitHub account are excluded, as are bots) and is typically much smaller than the engagement-based figure, because many people participate through issues and discussion without ever landing a commit. Both are legitimate; they answer different questions and should never be conflated.

Headline metric (`new_contributors`): Current window: **8** vs. 6 months earlier: **15** (change: -46.7%)

| window_end | new_contributors | new_commit_contributors |
| --- | --- | --- |
| 2024-10-01 | 30 | 10 |
| 2025-01-01 | 29 | 11 |
| 2025-04-01 | 14 | 6 |
| 2025-07-01 | 15 | 2 |
| 2025-10-01 | 19 | 2 |
| 2026-01-01 | 15 | 5 |
| 2026-04-01 | 10 | 3 |
| 2026-07-01 | 8 | 2 |

