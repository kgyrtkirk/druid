#!/bin/bash

set -e
gh pr list -S 'label:upstream -label:processed is:merged' -L1 --json number --jq '.[].number' | while read pr;do
	echo "processing #$pr"
	branch="`gh pr view $pr --json  headRefName --jq '.headRefName'`"
	echo "branch: $branch"
	gh pr edit $pr --add-label processed
	git push origin --delete "$branch" || true
done
