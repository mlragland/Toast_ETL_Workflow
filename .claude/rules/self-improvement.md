# Self-Improvement Protocol

## When Corrected by the User
After any correction or mistake:
1. **Reflect**: What went wrong? What assumption was incorrect?
2. **Abstract**: What is the general pattern behind this specific mistake?
3. **Generalize**: Turn it into a reusable decision rule
4. **Persist**: Write it to the appropriate location:
   - Coding pattern → add to relevant `.claude/rules/*.md` file
   - Project context → save to memory via `memory/` files
   - Deploy gotcha → add to `deploy-safety.md`
   - Business rule error → add to CLAUDE.md "Key Business Rules" section

## Rule Writing Format
When adding new rules:
- Use absolute directives: start with "NEVER" or "ALWAYS"
- Lead with WHY (1-2 sentences max) before the rule
- Be concrete — include actual commands, file paths, or code patterns
- One point per bullet — no compound rules

## When to Update CLAUDE.md vs Rules vs Memory
- **CLAUDE.md**: Only for critical project-level context that every session needs immediately
- **`.claude/rules/`**: Modular instructions — coding patterns, deploy procedures, guardrails
- **Memory files**: Session-to-session continuity — project status, user preferences, lessons learned

## Pruning
- Periodically review rules for relevance — remove anything Claude now does correctly without instruction
- If a rule has been followed consistently for 5+ sessions, it may be safe to remove
- Keep total instruction count under 150 across all files
- Remove rules that duplicate what's already in code (linters, type checking, tests)

## The Goal
Every mistake becomes a permanent lesson. The system should get better with every conversation.
