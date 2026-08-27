name: Verification Engine — archive_ii + archive_iii

# Scans archive_ii (ats + slug boards) and archive_iii (in-house career
# pages) and removes ONLY rows confirmed dead — the board/page genuinely
# doesn't exist anymore, never just "zero jobs right now" (see
# verification.py's own module docstring for the full rule and the
# 2026-08 research behind which of the 29 ATS platforms have a safe
# not-found signal — 18 do, 11 explicitly don't and are skipped/left
# untouched).
#
# Same shard + finalize shape as Main/main.py's ATS scanner: N parallel
# shards each check their own slice and upload a small JSON summary as a
# build artifact, then a `finalize` job (needs: verify) downloads every
# shard's summary and prints ONE combined report — active/empty/dead/
# unverified totals across the whole run.
on:
  workflow_dispatch:
    inputs:
      table:
        description: "Which table(s) to verify"
        required: false
        type: choice
        options:
          - both
          - archive_ii
          - archive_iii
        default: both
      execute:
        description: "Delete confirmed-dead rows (unchecked = report only)"
        required: false
        type: boolean
        default: false
      ats:
        description: "archive_ii only — restrict to one ATS platform. Leave blank for all."
        required: false
        default: ""
      shard_count:
        description: "Number of parallel shards"
        required: false
        default: "10"
      concurrency:
        description: "Concurrent checks in flight, per shard"
        required: false
        default: "30"

concurrency:
  group: verification-engine-${{ github.ref }}
  cancel-in-progress: false

jobs:
  prepare-matrix:
    # Same reason as kaggle_probe.yml's prepare-matrix job — workflow_dispatch
    # inputs can't be referenced directly inside a matrix, so shard_count ->
    # [0, 1, ..., shard_count-1] is computed here first.
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.build.outputs.matrix }}
    steps:
      - name: Build shard-index matrix from shard_count input
        id: build
        run: |
          SHARD_COUNT="${{ github.event.inputs.shard_count || '10' }}"
          MATRIX_JSON=$(python3 -c "
          import json
          shard_count = int('$SHARD_COUNT')
          print(json.dumps({'include': [{'shard': s} for s in range(shard_count)]}))
          ")
          echo "shard_count: $SHARD_COUNT"
          echo "matrix=$MATRIX_JSON" >> "$GITHUB_OUTPUT"

  verify:
    needs: prepare-matrix
    runs-on: ubuntu-latest
    timeout-minutes: 90
    strategy:
      fail-fast: false   # one shard's flakiness shouldn't cancel the rest
      matrix: ${{ fromJson(needs.prepare-matrix.outputs.matrix) }}

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install aiohttp aiodns selectolax requests beautifulsoup4 python-dotenv

      - name: "Verify shard ${{ matrix.shard }}/${{ github.event.inputs.shard_count || '10' }}"
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          # ats_scrapers.py (imported for the active/empty job-count split)
          # pulls in Main/config.py, which requires SOME LLM key to even
          # finish importing even though nothing here calls an LLM — reusing
          # the same secret daily_scan.yml already has configured is enough
          # to satisfy that import. See verification.py's module docstring.
          CEREBRAS_API_KEY: ${{ secrets.CEREBRAS_API_KEY }}
        run: |
          python Verification/verification.py \
            --table ${{ github.event.inputs.table || 'both' }} \
            --concurrency ${{ github.event.inputs.concurrency || '30' }} \
            --shard-index ${{ matrix.shard }} \
            --shard-count ${{ github.event.inputs.shard_count || '10' }} \
            --summary-out "summary-${{ matrix.shard }}.json" \
            ${{ github.event.inputs.execute == 'true' && '--execute' || '' }} \
            ${{ github.event.inputs.ats != '' && format('--ats {0}', github.event.inputs.ats) || '' }}

      - name: Upload shard summary
        uses: actions/upload-artifact@v4
        with:
          name: verification-summary-${{ matrix.shard }}
          path: summary-${{ matrix.shard }}.json
          retention-days: 14

  finalize:
    needs: verify
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install aiohttp aiodns selectolax requests beautifulsoup4 python-dotenv

      - name: Download every shard's summary
        uses: actions/download-artifact@v4
        with:
          pattern: verification-summary-*
          path: summaries
          merge-multiple: true

      - name: Combined report — all shards
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          CEREBRAS_API_KEY: ${{ secrets.CEREBRAS_API_KEY }}
        run: python Verification/verification.py --summarize summaries
