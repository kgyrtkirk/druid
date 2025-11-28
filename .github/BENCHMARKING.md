# Benchmark Comparison Guide

This repository uses JMH benchmarks with automated result storage for performance comparison across commits.

## Overview

The benchmark system works by:
1. Running JMH benchmarks and outputting results as JSON
2. Storing the JSON file in a separate repository (`kgyrtkirk/druid-bench`), named by commit SHA
3. Branch pointers (symlinks) track the latest result for each branch (only updated on fast-forward)
4. Accessing stored results via GitHub Pages URLs
5. Using jmh.morethan.io to visualize and compare results from different commits

## Running Benchmarks

### Via GitHub Actions

Benchmarks run automatically after unit tests pass on all branches and PRs in the main CI workflow.

The workflow will:
- Build the benchmarks module
- Run the SPL expression benchmarks
- Upload results to `kgyrtkirk/druid-bench` repository with the commit SHA
- Create/update a branch symlink (only if the new commit is a descendant of the old one)
- Print URLs for viewing and comparing results

Results include:
- Direct link to view this commit's results
- Comparison link against base branch (for PRs)
- Comparison link against merge-base (for PRs)

## Storage Format

Results are stored in the `kgyrtkirk/druid-bench` repository under `benchmark-results/`:

**By commit SHA:**
```
benchmark-results/{commit-sha}.json
```
Each commit gets its own file, immutable and permanent.

**By branch (symlinks):**
```
benchmark-results/branch-{branch-name}.json -> {commit-sha}.json
```
Symlinks point to the latest benchmark result for each branch. Only updated if the new commit is a descendant of the old one (fast-forward only).

**Access via GitHub Pages:**
```
https://kgyrtkirk.github.io/druid-bench/benchmark-results/{commit-sha}.json
https://kgyrtkirk.github.io/druid-bench/benchmark-results/branch-{branch-name}.json
```

## Comparing Results

The CI workflow automatically prints comparison links in the job output. For PRs, you get:
- Link to view the PR commit results
- Link to compare against the base branch HEAD
- Link to compare against the merge-base (where the PR branched off)

### Using jmh.morethan.io

jmh.morethan.io is a web-based tool for visualizing JMH benchmark results. It loads JSON files from URLs and displays them with charts and statistics.

**View a single commit:**
```
https://jmh.morethan.io/?source=https://kgyrtkirk.github.io/druid-bench/benchmark-results/{commit-sha}.json
```

**Compare multiple commits:**
```
https://jmh.morethan.io/?sources=URL1,URL2,URL3
```

### Manual Comparisons

**Compare any two commits:**
```
https://jmh.morethan.io/?sources=https://kgyrtkirk.github.io/druid-bench/benchmark-results/{commit-sha-1}.json,https://kgyrtkirk.github.io/druid-bench/benchmark-results/{commit-sha-2}.json
```

**Compare against a branch HEAD:**
```
https://jmh.morethan.io/?sources=https://kgyrtkirk.github.io/druid-bench/benchmark-results/{your-commit}.json,https://kgyrtkirk.github.io/druid-bench/benchmark-results/branch-main.json
```

**Track performance over time:**
Get commit SHAs from git history and construct URLs for each:
```bash
git log --oneline -n 10 main
```
Then paste multiple URLs into jmh.morethan.io separated by commas.

## Setup Requirements

- Separate repository `kgyrtkirk/druid-bench` must exist
- GitHub Pages must be enabled for `kgyrtkirk/druid-bench` (Settings → Pages)
- GitHub Secret `BENCH_REPO_TOKEN` must be configured:
  - Create a Personal Access Token (PAT) with write access to `kgyrtkirk/druid-bench`
  - Add it as a repository secret named `BENCH_REPO_TOKEN` in the main repository's settings

## Implementation Details

### Scripts

**`.github/scripts/run_benchmark`**
- Builds the benchmarks module
- Runs SPL expression benchmarks
- Uploads results to `kgyrtkirk/druid-bench` repository
- Prints comparison URLs

**`.github/scripts/upload_benchmark_results`**
- Clones/updates the `kgyrtkirk/druid-bench` repository
- Stores JSON file by commit SHA
- Creates/updates branch symlink (if `BRANCH_NAME` is set)
- Only updates symlink on fast-forward (checks ancestry)
- Pushes to the separate benchmark repository

**`extensions-imply/imply-obsware/dev/run_expr_benchmark`**
- Runs JMH benchmarks for SPL expressions
- Outputs `benchmark-results.json` in JSON format
- Accepts standard JMH command-line options

### Environment Variables

The CI workflow sets these variables:
- `CI_COMMIT_SHA` - The actual commit SHA (for PRs, this is the PR head, not the merge commit)
- `CI_BRANCH_NAME` - Branch name (enables symlink creation/update)
- `CI_BASE_BRANCH` - Base branch for comparisons (empty for non-PR pushes)
- `BENCH_REPO_TOKEN` - GitHub Personal Access Token for pushing to the benchmark repository (from secrets)

## Troubleshooting

**Benchmark repository doesn't exist:**
Create the `kgyrtkirk/druid-bench` repository and enable GitHub Pages:
```bash
# Create new repository at https://github.com/kgyrtkirk/druid-bench
# Then initialize it:
git clone https://github.com/kgyrtkirk/druid-bench.git
cd druid-bench
mkdir benchmark-results
echo "<h1>Benchmark Results</h1>" > index.html
git add .
git commit -m "Initialize benchmark repository"
git push origin main
# Enable GitHub Pages in repository settings (Settings → Pages, source: main branch)
```

**404 errors accessing JSON URLs:**
- Check the `kgyrtkirk/druid-bench` repository exists and has the file
- Verify GitHub Pages is enabled for the repository
- Check: `https://github.com/kgyrtkirk/druid-bench/tree/main/benchmark-results`

**Branch symlink not updating:**
By design. Symlinks only update on fast-forward (when old commit is ancestor of new commit). This prevents the branch pointer from jumping backwards or to unrelated commits.

**Authentication errors when pushing:**
- Verify `BENCH_REPO_TOKEN` secret is configured in repository settings
- Ensure the PAT has write permissions to `kgyrtkirk/druid-bench`
- Check that the PAT hasn't expired

## Why This Design

**Separate repository with GitHub Pages:**
- Keeps benchmark results separate from main codebase
- Publicly accessible URLs
- Permanent storage without cluttering main repository
- Works with jmh.morethan.io

**Commit SHA as filename:**
- Immutable identifier for specific code state
- No conflicts when multiple commits run concurrently
- Easy to find historical results

**Branch symlinks:**
- Quick access to latest results for a branch
- Fast-forward only prevents confusion from force pushes
- Compare PRs against base branch HEAD without knowing specific commits
