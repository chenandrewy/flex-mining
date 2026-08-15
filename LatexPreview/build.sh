#!/usr/bin/env bash
# Run: ./LatexPreview/build.sh
# Inputs: LatexPreview/exhibits.tex and the generated exhibits in ../Results.
# Output: LatexPreview/exhibits.pdf.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
build_dir="$(mktemp -d)"
trap 'rm -rf "$build_dir"' EXIT

cd "$script_dir"
pdflatex -interaction=nonstopmode -halt-on-error -output-directory="$build_dir" exhibits.tex
pdflatex -interaction=nonstopmode -halt-on-error -output-directory="$build_dir" exhibits.tex

staged_pdf="$(mktemp "$script_dir/exhibits.pdf.XXXXXX")"
cp "$build_dir/exhibits.pdf" "$staged_pdf"
mv -f "$staged_pdf" "$script_dir/exhibits.pdf"
