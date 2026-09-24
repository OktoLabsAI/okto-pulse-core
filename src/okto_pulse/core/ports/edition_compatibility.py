"""Public edition API revision, independent of storage and runtime mechanics.

Advance this revision when a coordinated edition cannot safely compose the
Core ports or payloads. Matching package versions alone do not identify this
contract during development or a migration between historical builds.
"""

EDITION_API_CONTRACT = "pulse-edition-api/4"
