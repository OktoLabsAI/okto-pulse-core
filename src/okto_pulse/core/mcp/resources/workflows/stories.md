---
version: "1.0"
---

# Stories & Topics — Pre-Ideation Intake

## 2.0 Stories & Topics — optional pre-ideation intake

Stories are lightweight, optional intake items inspired by user stories. They precede ideation and are grouped by a board-scoped Topic. Use them when the user gives raw needs, multiple user perspectives, or backlog snippets that are not yet ready to become a single ideation.

**Tools and permissions:**

| Action | Tool | Required permission |
|---|---|---|
| List Topics | `okto_pulse_list_topics` | `topic.entity.read` |
| Create Topic | `okto_pulse_create_topic` | `topic.entity.create` |
| Update Topic fields | `okto_pulse_update_topic` | `topic.entity.edit_fields` |
| Archive Topic | `okto_pulse_archive_topic` | `topic.entity.archive` |
| Restore Topic | `okto_pulse_restore_topic` | `topic.entity.restore` |
| Delete empty Topic | `okto_pulse_delete_topic` | `topic.entity.delete` |
| Merge Topics | `okto_pulse_merge_topics` | `topic.entity.merge` |
| List Stories | `okto_pulse_list_stories` | `story.entity.read` |
| Create Story | `okto_pulse_create_story` | `story.entity.create` |
| Update Story fields/labels | `okto_pulse_update_story` | `story.entity.edit_fields` and/or `story.entity.label` |
| Move Story | `okto_pulse_move_story` | matching `story.move.*` flag + `story.interact_in.<current_status>` |
| Archive Story | `okto_pulse_archive_story` | `story.entity.archive` |
| Restore Story | `okto_pulse_restore_story` | `story.entity.restore` |
| Link Story to Ideation | `okto_pulse_link_story_to_ideation` | `story.links.ideation` |
| Convert Stories to Ideation | `okto_pulse_convert_stories_to_ideation` | `story.conversion.to_ideation` |

**Topic rules:**
- Reuse an existing Topic when it names the same product area or backlog theme.
- Create a new Topic only when no existing Topic matches the user's grouping language.
- Update, archive, restore, delete and merge must use the dedicated Topic tools. Do not simulate merge by silently rewriting Stories one by one.
- `okto_pulse_delete_topic` is safe-delete only: it succeeds only when the Topic has no active or archived Stories. If it returns `topic_not_empty`, explain the active/archived counts and suggest merge, moving Stories or archiving instead of retrying blindly.
- `okto_pulse_merge_topics` moves every Story from source to target, preserves Story-Ideation links, archives the source Topic, and returns an `impact` object. Cite that impact when confirming the operation to the user.
- Topic archive/restore affects only the grouping entity. It must not be described as archiving or restoring the Stories inside it.

**Story content rules:**
- Write Story text as a user need, not as a solution spec.
- Fill `actor`, `goal`, and `benefit` when the user provides them or they are directly inferable from the story sentence. Leave unknown fields empty instead of inventing a persona.
- Use `labels` for cross-cutting tags such as `resource-gate`, `security`, `ux`, or `analytics`; use Topic for the primary grouping.
- Mockups attached to Stories are optional first-class context. Manage them with `okto_pulse_add_screen_mockup`, `okto_pulse_update_screen_mockup`, `okto_pulse_annotate_mockup`, `okto_pulse_list_screen_mockups`, and `okto_pulse_delete_screen_mockup` using `entity_type="story"`. When a Story becomes an Ideation/Spec, propagate or recreate only the mockups that remain relevant.

**Story/Topic operation pre-flight:**
- Before link, convert, move, archive, merge, or delete, list the affected Topics and candidate Stories with `okto_pulse_list_topics` and `okto_pulse_list_stories(include_links="true", include_archived="true")`.
- Inspect each Story's `status`, `archived`, and `ideation_links` before choosing the operation. Do not operate from memory or from a title-only match.
- For link/convert, verify every candidate Story is `ready`, not archived, and has no existing `ideation_links`; also verify the target Ideation is editable before calling `okto_pulse_link_story_to_ideation`.
- A Story can link to at most one Ideation. Repeating the same Story + Ideation link returns `already linked`; linking the same Story to a different Ideation is rejected. Multiple Stories may link to the same Ideation when they converge on the same problem space.
- For merge/delete/archive decisions, use the Topic `impact`/counts and explain whether Stories were moved, preserved, or left untouched.

**Status flow:**

| Status | Meaning | Normal next actions |
|---|---|---|
| `draft` | Raw intake, still rough | edit, move to `triage` or `ready` |
| `triage` | Being reviewed/organized | move to `draft` for rework or `ready` |
| `ready` | Good enough to feed ideation | link to an existing Ideation or convert to a new Ideation |
| `converted` | Terminal result of successful link/conversion | read only for normal flow; do not move out |

`converted` is not a normal manual lifecycle move. It is set by a successful Story-Ideation link or conversion path. `okto_pulse_link_story_to_ideation` always marks the Story as `converted`; the `mark_converted` argument is compatibility-only and does not preserve `ready`. If `okto_pulse_move_story(status="converted")` fails, do not retry with broader permissions; use `okto_pulse_link_story_to_ideation` or `okto_pulse_convert_stories_to_ideation` after the Story is `ready`.

**Derivation guidance:**
- Several Stories can feed one Ideation when they describe the same problem space.
- One Story cannot link to more than one Ideation. If a Story seems to inform multiple solution tracks, create or select the single consolidation Ideation first, then split downstream through refinements/specs as needed.
- Before converting, list existing Ideations and prefer linking to an editable/resolvable match over creating a duplicate. Story-Ideation links are allowed only when the target Ideation is editable (`draft`, `review`, `approved`, or `evaluating`); do not link Stories to `done`, `cancelled`, archived, already linked Stories, or to the same Ideation pair twice.
- Once a Story is converted, treat it as historical lineage context. Do not edit it to match the downstream Ideation; refinements/specs are where solution detail is sharpened.
