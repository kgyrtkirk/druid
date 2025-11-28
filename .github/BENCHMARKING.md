# Benchmark Comparison Guide

This repository uses JMH benchmarks with automated result storage for performance comparison across commits.

## Overview

The benchmark system works by:
1. Running JMH benchmarks and outputting results as JSON
2. Storing the JSON file in the `gh-pages` branch, named by commit SHA
3. Branch pointers (symlinks) track the latest result for each branch (only updated on fast-forward)
4. Accessing stored results via GitHub Pages URLs
5. Using jmh.morethan.io to visualize and compare results from different commits

## Running Benchmarks

### Via GitHub Actions

Benchmarks run automatically after unit tests pass on all branches and PRs in the main CI workflow.

The workflow will:
- Build the benchmarks module
- Run the SPL expression benchmarks
- Upload results to gh-pages branch with the commit SHA
- Create/update a branch symlink (only if the new commit is a descendant of the old one)
- Print URLs for viewing and comparing results

Results include:
- Direct link to view this commit's results
- Comparison link against base branch (for PRs)
- Comparison link against merge-base (for PRs)

## Storage Format

Results are stored in the `gh-pages` branch under `benchmark-results/`:

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
https://implydata.github.io/druid/benchmark-results/{commit-sha}.json
https://implydata.github.io/druid/benchmark-results/branch-{branch-name}.json
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
https://jmh.morethan.io/?source=https://implydata.github.io/druid/benchmark-results/{commit-sha}.json
```

**Compare multiple commits:**
```
https://jmh.morethan.io/?sources=URL1,URL2,URL3
```

### Manual Comparisons

**Compare any two commits:**
```
https://jmh.morethan.io/?sources=https://implydata.github.io/druid/benchmark-results/{commit-sha-1}.json,https://implydata.github.io/druid/benchmark-results/{commit-sha-2}.json
```

**Compare against a branch HEAD:**
```
https://jmh.morethan.io/?sources=https://implydata.github.io/druid/benchmark-results/{your-commit}.json,https://implydata.github.io/druid/benchmark-results/branch-main.json
```

**Track performance over time:**
Get commit SHAs from git history and construct URLs for each:
```bash
git log --oneline -n 10 main
```
Then paste multiple URLs into jmh.morethan.io separated by commas.

## Setup Requirements

- Enable GitHub Pages (Settings → Pages, source: gh-pages branch)
- Workflow needs write permissions to push to gh-pages

## Implementation Details

### Scripts

**`.github/scripts/run_benchmark`**
- Builds the benchmarks module
- Runs SPL expression benchmarks
- Uploads results to gh-pages
- Prints comparison URLs

**`.github/scripts/upload_benchmark_results`**
- Stores JSON file by commit SHA
- Creates/updates branch symlink (if `BRANCH_NAME` is set)
- Only updates symlink on fast-forward (checks ancestry)
- Pushes to gh-pages branch

**`extensions-imply/imply-obsware/dev/run_expr_benchmark`**
- Runs JMH benchmarks for SPL expressions
- Outputs `benchmark-results.json` in JSON format
- Accepts standard JMH command-line options

### Environment Variables

The CI workflow sets these variables:
- `COMMIT_SHA` - The actual commit SHA (for PRs, this is the PR head, not the merge commit)
- `BRANCH_NAME` - Branch name (enables symlink creation/update)
- `BASE_BRANCH` - Base branch for comparisons (empty for non-PR pushes)
- `REPO_OWNER` - Repository owner for URL construction
- `REPO_NAME` - Repository name for URL construction

## Troubleshooting

**gh-pages branch doesn't exist:**
The first benchmark run will fail. Create it:
```bash
git checkout --orphan gh-pages
git rm -rf .
mkdir benchmark-results
echo "<h1>Benchmark Results</h1>" > index.html
git add .
git commit -m "Initialize gh-pages"
git push origin gh-pages
```

**404 errors accessing JSON URLs:**
- Check the gh-pages branch exists and has the file
- Verify: `https://github.com/implydata/druid/tree/gh-pages/benchmark-results`

**Branch symlink not updating:**
By design. Symlinks only update on fast-forward (when old commit is ancestor of new commit). This prevents the branch pointer from jumping backwards or to unrelated commits.

## Why This Design

**gh-pages with GitHub Pages:**
- Publicly accessible URLs
- Permanent storage
- Works with jmh.morethan.io

**Commit SHA as filename:**
- Immutable identifier for specific code state
- No conflicts when multiple commits run concurrently
- Easy to find historical results

**Branch symlinks:**
- Quick access to latest results for a branch
- Fast-forward only prevents confusion from force pushes
- Compare PRs against base branch HEAD without knowing specific commits
