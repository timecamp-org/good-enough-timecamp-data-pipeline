---
description: 
globs: 
alwaysApply: true
---
# Cursor Development Rules & AI Collaboration Guide

## 📜 Core Philosophy

1.  **Simplicity:** Prioritize simple, clear, and maintainable solutions. Avoid unnecessary complexity or over-engineering.
2.  **Iterate:** Prefer iterating on existing, working code rather than building entirely new solutions from scratch, unless fundamentally necessary or explicitly requested.
3.  **Focus:** Concentrate efforts on the specific task assigned. Avoid unrelated changes or scope creep.
4.  **Quality:** Strive for a clean, organized, well-tested, and secure codebase.
5.  **Collaboration:** This document guides both human developers and the AI assistant for effective teamwork.

## 📚 Project Context & Understanding

1.  **Documentation First:**
    *   **Always** check for and thoroughly review relevant project documentation *before* starting any task. This includes:
        *   Product Requirements Documents (PRDs)
        *   `README.md` (Project overview, setup, patterns, technology stack)
        *   `docs/architecture.md` (System architecture, component relationships)
        *   `docs/technical.md` (Technical specifications, established patterns)
        *   `tasks/tasks.md` (Current development tasks, requirements)
    *   If documentation is missing, unclear, or conflicts with the request, **ask for clarification**.
2.  **Architecture Adherence:**
    *   Understand and respect module boundaries, data flow, system interfaces, and component dependencies outlined in `docs/architecture.md`.
    *   Validate that changes comply with the established architecture. Warn and propose compliant solutions if a violation is detected.
3.  **Pattern & Tech Stack Awareness:**
    *   Reference `README.md` and `docs/technical.md` to understand and utilize existing patterns and technologies.
    *   Exhaust options using existing implementations before proposing new patterns or libraries.

## ⚙️ Task Execution & Workflow

1.  **Task Definition:**
    *   Clearly understand the task requirements, acceptance criteria, and any dependencies from `tasks/tasks.md` and the PRD.
2.  **Systematic Change Protocol:** Before making significant changes:
    *   **Identify Impact:** Determine affected components, dependencies, and potential side effects.
    *   **Plan:** Outline the steps. Tackle one logical change or file at a time.
    *   **Verify Testing:** Confirm how the change will be tested. Add tests if necessary *before* implementing (see TDD).
3.  **Progress Tracking:**
    *   Keep `docs/status.md` updated with task progress (in-progress, completed, blocked), issues encountered, and completed items.
    *   Update `tasks/tasks.md` upon task completion or if requirements change during implementation.

## 🤖 AI Collaboration & Prompting

1.  **Clarity is Key:** Provide clear, specific, and unambiguous instructions to the AI. Define the desired outcome, constraints, and context.
2.  **Context Referencing:** If a task spans multiple interactions, explicitly remind the AI of relevant previous context, decisions, or code snippets.
3.  **Suggest vs. Apply:** Clearly state whether the AI should *suggest* a change for human review or *apply* a change directly (use only when high confidence and task is well-defined). Use prefixes like "Suggestion:" or "Applying fix:".
4.  **Question AI Output:** Human developers should critically review AI-generated code. Question assumptions, verify logic, and don't blindly trust confident-sounding but potentially incorrect suggestions (hallucinations).
5.  **Focus the AI:** Guide the AI to work on specific, focused parts of the task. Avoid overly broad requests that might lead to architectural or logical errors.
6.  **Leverage Strengths:** Use the AI for tasks it excels at (boilerplate generation, refactoring specific patterns, finding syntax errors, generating test cases) but maintain human oversight for complex logic, architecture, and security.
7.  **Incremental Interaction:** Break down complex tasks into smaller steps for the AI. Review and confirm each step before proceeding.
8.  **Standard Check-in (for AI on large tasks):** Before providing significant code suggestions:
    *   "Confirming understanding: I've reviewed [specific document/previous context]. The goal is [task goal], adhering to [key pattern/constraint]. Proceeding with [planned step]." (This replaces the more robotic "STOP AND VERIFY").

## ✨ Code Quality & Style

2.  **Readability & Maintainability:** Write clean, well-organized code.
4.  **Avoid Duplication (DRY):** Actively look for and reuse existing functionality. Refactor to eliminate duplication.
7.  **Pattern Consistency:** Adhere to established project patterns. Don't introduce new ones without discussion/explicit instruction. If replacing an old pattern, ensure the old implementation is fully removed.
8.  **File Naming:** Use clear, descriptive names. Avoid "temp", "refactored", "improved", etc., in permanent file names.

## ♻️ Refactoring

1.  **Purposeful Refactoring:** Refactor to improve clarity, reduce duplication, simplify complexity, or adhere to architectural goals.
2.  **Holistic Check:** When refactoring, look for duplicate code, similar components/files, and opportunities for consolidation across the affected area.
4.  **Verify Integrations:** After refactoring, ensure all callers, dependencies, and integration points function correctly. Run relevant tests.

## ✅ Testing & Validation

1. You can run the Python sciprt to test them
2. You can add --debug param

## 🐛 Debugging & Troubleshooting

1.  **Fix the Root Cause:** Prioritize fixing the underlying issue causing an error, rather than just masking or handling it, unless a temporary workaround is explicitly agreed upon.
2.  **Console/Log Analysis:** Always check browser and server console output for errors, warnings, or relevant logs after making changes or when debugging. Report findings.
4.  **Check the `fixes/` Directory:** Before deep-diving into a complex or recurring bug, check `fixes/` for documented solutions to similar past issues.
5.  **Document Complex Fixes:** If a bug requires significant effort (multiple iterations, complex logic) to fix, create a concise `.md` file in the `fixes/` directory detailing the problem, investigation steps, and the solution. Name it descriptively (e.g., `fixes/resolve-race-condition-in-user-update.md`).

## 🔒 Security

1.  **Server-Side Authority:** Keep sensitive logic, validation, and data manipulation strictly on the server-side. Use secure API endpoints.
2.  **Input Sanitization/Validation:** Always sanitize and validate user input on the server-side.
3.  **Dependency Awareness:** Be mindful of the security implications of adding or updating dependencies.
4.  **Credentials:** Never hardcode secrets or credentials in the codebase. Use environment variables or a secure secrets management solution.

## 📄 Documentation Maintenance

1.  **Update Docs:** If code changes impact architecture, technical decisions, established patterns, or task status, update the relevant documentation (`README.md`, `docs/architecture.md`, `docs/technical.md`, `tasks/tasks.md`, `docs/status.md`).


