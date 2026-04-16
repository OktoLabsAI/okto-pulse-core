# Contributing to Okto Pulse Core

Thanks for your interest in contributing to Okto Pulse Core! This document explains how to get involved.

## Before You Start

By submitting a pull request, you agree to our [Contributor License Agreement](./CLA.md). No separate signature is required — opening a PR implies acceptance.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/OktoLabsAI/okto-pulse-core.git
cd okto-pulse-core

# Install Python dependencies (requires Python 3.11+)
pip install -e ".[dev]"

# Run tests
pytest
```

## How to Contribute

### Reporting Bugs

Open an issue using the **Bug Report** template. Include:
- Steps to reproduce
- Expected vs actual behavior
- Python version and OS
- Relevant logs or error tracebacks

### Suggesting Features

Open an issue using the **Feature Request** template. Describe:
- The problem you're trying to solve
- Your proposed solution
- Alternatives you've considered

### Submitting Code

1. **Fork** the repo and create a branch from `main`
2. **Name your branch** descriptively: `fix/issue-123-description` or `feat/new-feature`
3. **Make your changes** — keep PRs focused on a single concern
4. **Write tests** for new functionality
5. **Run the test suite** before submitting
6. **Open a PR** against `main` with a clear description

### Pull Request Guidelines

- Keep PRs small and focused — one concern per PR
- Write clear commit messages (we follow [Conventional Commits](https://www.conventionalcommits.org/))
- Include tests for new features and bug fixes
- Update documentation if your change affects the API
- All CI checks must pass before merge
- A maintainer will review your PR — be patient, we're a small team

### Commit Message Format

```
type(scope): short description

Optional longer description.

Closes #123
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

## Project Structure

```
okto-pulse-core/
├── src/okto_pulse/       # Core engine (FastAPI, SQLAlchemy, MCP)
│   ├── models/           # SQLAlchemy models
│   ├── services/         # Business logic
│   ├── routes/           # API endpoints
│   └── mcp/              # MCP server and tools
├── LICENSE               # Elastic License 2.0
├── CLA.md                # Contributor License Agreement
└── TRADEMARKS.md         # Trademark policy
```

## Code of Conduct

Be respectful, constructive, and inclusive. We're building something together — disagreements are fine, disrespect is not.

## License

Okto Pulse Core is source-available under the [Elastic License 2.0](./LICENSE). By contributing, you agree that your contributions will be licensed under the same terms, as described in the [CLA](./CLA.md).

## Questions?

- Open a [Discussion](https://github.com/OktoLabsAI/okto-pulse-core/discussions) for questions
- Email: contact@oktolabs.ai
