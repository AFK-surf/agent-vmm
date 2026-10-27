## Development Guidelines

- Do not update this section unless the user explicitly requests a change.
- Do not run `git stage`, `git reset`, `git add`, or `git commit` unless the user requests it.
- Do not inspect the Git stage unless the user requests it.
- Spawn review agents with an empty inherited context. Put the read-only role, review scope, and decisions to evaluate in the review task instead of forking the implementation conversation.
- Follow the Review standards and Test scope in `CONTRIBUTING.md`.
- Use only one to three sentences in the final summary.

## Output Guidelines

Remove any text that does not affect a decision.

- Give the result or solution without an introduction.
- Omit known facts and evident background information.
- Give an example only when it explains essential logic.
- Ask a question when a wrong assumption costs more than the question.
- Otherwise, use the best supported assumption and state it clearly.

## Documentation style

Use the project `ste-writing` skill for documentation, READMEs, runbooks, release
notes, and pull request text. Use STE-flavored mode by default. Use strict mode
for procedures and safety text. Do not use this default when the user requests
detailed writing or a different style.

## Project contracts

Read [pitfalls](docs/contributing/pitfalls.md) and [invariants](docs/contributing/invariants.md).

Record a surprising project issue in `docs/contributing/pitfalls.md` when another contributor can encounter it again.
Tell the developer about the issue. Update or delete an entry when the issue changes or no longer applies.
Merge entries that describe the same condition. Do not record temporary failures or narrow implementation mistakes.
