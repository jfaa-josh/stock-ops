# Project Overview

🚨 Describe 🚨

# Stock-Ops MLOps Overview
This repository implements an MLOps workflow designed to support reproducible, testable, and collaborative development using dependency and environment management, pre-commit enforcement, and GitLab CI/CD.

## 1. Local Development
Developers begin by creating a feature branch from main. All new functionality, bug fixes, and experiments are done in these isolated branches.
Work is done locally using a uv-managed local development environment. This allows developers to include packages for their preferred IDEs and tools while reducing production dependency bloat. Production dependencies resemble this in `uv.lock` and a pre-commit hook references this list for CI/CD.

## 2. Pre-Commit Enforcement

Before local changes are committed, pre-commit hooks, managed by the python `pre-commit` package, run automatically. The following checks are performed:

- `ruff`, `mypy`: Style, import order, linting, and type checks on source and test code
- `check-added-large-files`, `check-merge-conflict`,`trailing-whitespace`, `end-of-file-fixer`, `check-toml`: Basic file hygiene checks

These hooks run locally before each commit. Commits will fail until all checks pass.

## 3. Data Handling

Data is organized and managed based on size and role in the development process:

- **Small test data** (e.g., fixtures, smoke test inputs) are stored in data/git_lfs/ and tracked using [Git LFS](https://git-lfs.com/). There is a 1GB cap on this remote storage option. These are pulled with:

- **Large datasets** for model development (e.g., training, validation) are stored remotely and referenced in data/raw/ and data/preprocessed/. [- These will be managed using DVC, with logic for dvc pull to be added to future CI steps. -]

## 4. Model Handling

🚨 TO BE COMPLETED...🚨

## 5. Push to Remote and CI

Pushing to a remote feature branch triggers the GitLab CI pipeline, defined in `.gitlab-ci.yml`. The pipeline performs the following pre-commit commands within a single stage via `pre-commit run --all`:

**Setup:** Creates a clean environment and pip installs dev dependencies

**Linting & Type Checks:** Enforces code quality standards

**Testing:** Executes unit and integration tests with coverage reports

Note: The pipeline is configured as a single stage due to NASA GitLab runner restrictions, which prevent passing artifacts between stages.

## 6. Merging to Main

Once development is complete and all tests pass, changes are submitted via a merge request from the feature branch back to the main branch.

## 7. Deployment

🚨 TO BE COMPLETED... 🚨

# Repository Files Overview
Key repository files are described below:
- root/
    - **pyproject.toml:** a configuration file used by packaging tools, as well as other tools such as linters, type checkers, etc. including for `ruff` linting and formatting tool with settings as well as to ensure only relevant repository files are processed. Specify dependencies here.
    - **.python-version**: contains the project's default Python version. This file tells uv which Python version to use when creating the project's virtual environment.
    - **uv.lock**: a cross-platform lockfile that contains information about the exact resolved versions of your project's dependencies. It is managed by uv and should not be edited manually. This file should be checked into version control, allowing for consistent and reproducible installations across machines.
    - **.venv**: This folder contains your project's virtual environment, a Python environment that is isolated from the rest of your system. This is where uv will install your project's dependencies.
    🚨 - **.gitlab-ci.yml**: A configuration file that defines the CI/CD pipeline actions and executes `pre-commit` commands stored in the `.pre-commit-config.yaml` automatically when code is pushed or merged.
    🚨 - **.pre-commit-config.yaml**: A configuration file for the `pre-commit` framework that defines a set of hooks to run automatically before each commit. These hooks are run locally, prior to pushing code remotely, to enforce code quality standards such as formatting, linting, and validation.
    - **justfile**: File containing aliased commands for linting, formatting, fixing, and testing for ease of use. Similar to a Makefile.
    -  **mypy.ini:** Configuration file for `mypy`, static type checker for Python that analyzes code to ensure type annotations are used correctly, to ensure only /src and /tests codes are processed.
    - **.gitattributes**: A git configuration file used to control how Git handles specific file types, including text normalization, diff behavior, and integration with tools like Git LFS for large file tracking.
    - **.gitignore**: A git configuration file used to exclude files from being synced to the repository in order to avoid bloat (i.e., local data or configuration files).
    - **README.md**: Markdown for this readme file.
- data/
    - git_lfs/
        - Location for 10GB max storage for Ci required test and smoke test datasets managed by Git LFS.
    - raw/: location for large raw data sets.
    - processed/: location for large labeled test, train, and validation datasets.
        - test/
        - train/
        - validate/
- models/
    - 🚨 FUTURE 🚨
- src/
    - Python source code.
- tests/
    - Scripts to run in CI using Pytest-cov for source code validation:
    - `test_script.py`: 🚨 Template code tests 🚨

# Project-Specific Details

- Python=3.13 pinned for this repo.
- CI employes dependency caching in order to increase speed; the first time CI is run when new dependencies are specified may be slower
- Use `uv` to install packages, create virtual environments, install new python versions, and run QA tools.

# Installation & Setup

Follow the steps below to set up your local development environment for this repository.

1. **Install UV**

   Ensure that uv is installed on your machine. Follow the documentation for a standalone, separate from global python, installation for your specific OS [here](https://docs.astral.sh/uv/getting-started/installation/#installation-methods).

1. **Install Git Bash**

   Install git bash as your terminal interface.  It is recommended to use VSCode as your editor.

1. **Create a local Directory**

   Choose or create a folder to house the repository files.

   Navigate into it within git bash:

   - `cd /path/to/your/local/folder`

1. **Clone the Repository**

   Clone the repository using the SSH URL provided by the GitHub interface. Make sure you check out the main branch:

   - `git clone git@github.com:your-username/your-repo.git`
   - `git checkout main`

1. **Create a Local Development Environment**

   - Set up a local development environment using `uv venv .venv` in the root of the directory. The name `venv` is industry standard for the environment, and easy to .gitignore and autodiscover.

   - Source the virtual environment
   - Install packages from `uv.lock` by running: `uv sync`

1. **Initialize Pre-Commit (One-Time Setup)**

   Install pre-commit hooks locally, ensure git bash is in repo directory and conda environment is active:

   - `pre-commit install`

1. **Install Git LFS**

   Run the following once per computer:

   - `git lfs install`
   - `git lfs pull`

   This downloads all Git LFS-tracked files locally to data/git_lfs/.

1. **Test Your Install**

   Manually verify that pre-commit hooks are successful by running:

   - `pre-commit run --all`

# Developer Workflow

1. **Create a New Feature Branch**

   Create a new feature branch for local development following the instructions [here](#create-feature-branch).

1. **Coding Best Practices**

   - Always develop new code within a feature branch.
   - As new dependencies are required, add them to `pyproject.toml` and relock or use `uv add <package>` to add and install it automatically.
   - As new source code functionality is added, add tests to files in /tests to verify during CI.
   - Update `README.md` as needed.
   - [Push](#push-to-feature-branch) changes to feature branch frequently.
   - Occasionally [rebase](#rebase-with-main-branch) from main to ensure working code is current.

1. **Push Changes to Feature Branch**

   To manually verify that pre-commit hooks will be successful before pushing the repo:

   - `pre-commit run --all-files`

   Push new code to remote feature branch frequently following the instructions [here](#push-to-feature-branch).

   Occasionally rebase from main to ensure working code is up to date following instructions [here](#rebase-with-main-branch).

1. **Merge Feature Branch With Main Branch**

   When feature code is ready to integrate with main, rebase, and if CI succeeds submit a merge request following the instructions [here](#merge-feature-to-main-branch).

1. **Monitor CI**

   - CI will run on push and merge in any branch.
   - Can monitor progress in the online repo Build -> Jobs.
   - Upon completion, an email is sent with result.

# Git Workflow & Commands

## Create Feature Branch

   Ensure the local main branch is up to date:

   - `git checkout main`
   - `git fetch origin`
   - `git pull origin main`

   Create new feature branch. Use feature/ and dashes for readability (e.g. *feature/dev-work*):
   - `git checkout -b feature/<NAME>`

   Push new feature branch to upstream tracking:
   - `git push -u origin feature/<NAME>`

## Push to Feature Branch

   It is good practice to occisionally [rebase](#rebase-with-main-branch) to ensure that main branch code is current in the feature branch and modifications are not diverging the two branches.

   Ensure feature/NAME branch is active.  If not:
   - `git fetch origin`
   - `git checkout feature/<NAME>`

   Ensure local feature/NAME branch is synced with remote:
   - `git pull`

   Stage, commit, and push files:
   - `git add .` or more selectively `git add <files>`
   - `git commit -m "Descriptive commit message"`

   - If pre-commit hooks fail because files were modified, simply rerun the add and commit commands above.
   - If pre-commit hooks fail because of a bug, fix the bug, then rerun the add and commit commands above.

   - `git push origin feature/<NAME>`

   Ensure the working tree is clean and files are pushed:
   - `git status`

   Upon push, CI will begin running in the online branch repository. Instructions to **Monitor CI** and ensure checks pass are found [here](developer-workflow).

   Note, main is a protected branch, so direct push to main will be rejected.  **Please always ensure you are in the feature branch when pushing to remote.**

## Rebase With Main Branch

   Rebase re-applies commits from one branch onto another, producing a clean linear history. Useful for syncing feature branches with main, especially before merging, so that untracked changes that occured in main after the feature branch was created do not introduce messy conflicts later.

   Ensure feature/NAME branch is active.  If not:
   - `git fetch origin`
   - `git checkout feature/NAME`

   Rebase:
   - `git fetch origin`
   - `git rebase origin/main`

   If conflict:
   - Fix files manually, then...
   - `git add .`
   - `git rebase --continue`

   Push to update remote feature/NAME branch:
   - `git push --force-with-lease`

## Merge Feature to Main Branch

   Direct push to main branch is disabled in this repository.  To merge code from a feature branch to main:

   Ensure feature/NAME is up to date with main:
   - [Rebase](#rebase-with-main-branch) feature/NAME branch.

   Submit merge request:
   - Go to online GitLab repository.
   - Code -> merge requests.
   - Create new merge request.
   - Ensure branch and merging into branch (main) are correct (top of page).
   - Add Josh Fody as assignee and reviewer and submit.

   Once merge request is approved, CI will begin running in the online branch repository. Instructions to **Monitor CI** and ensure checks pass are found [here](developer-workflow). If CI is successful merge is complete.

   If delete original branch box is checked, the feature branch is deleted from the repo; if not, should manually delete from online repository.

   Post merge local actions:

   - Delete local feature branch:
      - `git branch` (to ensure branch still exists)
      - `git branch -D feature/<NAME>`

   - Sync local main with new merged main in online repo:
      - `git checkout main`
      - `git fetch origin`
      - `git pull origin main`
