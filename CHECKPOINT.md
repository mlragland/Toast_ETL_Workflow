# Session Checkpoint — Toast Pipeline

## Completed
- G&A subcategories updated: added share_pct allocations, removed informational flags, added actionable insights

## Next Task: Move Repairs & Maintenance to Facility & Build-Out

### Pre-flight checks before making changes:
1. Search main.py for ALL references to the repairs_maintenance subcategory key
2. Check what parent category it currently belongs to
3. Search for any logic that filters/groups by parent category
4. Check if share_pct values in destination (facility) still sum to 100% after adding it
5. Check if share_pct values in source category still sum to 100% after removing it
6. Search for any hardcoded references to repairs_maintenance in reports or dashboards

### Changes needed:
- Update the parent field from current value to facility
- Adjust share_pct in BOTH source and destination so each sums to 100%
- Verify keywords still make sense in the new grouping

### Rules:
- Do NOT start changes until all pre-flight checks are complete
- Show me the impact analysis before making edits
