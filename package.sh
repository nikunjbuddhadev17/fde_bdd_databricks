#!/usr/bin/env bash
set -euo pipefail

# Package this repo into fde_interview_packet.zip:
# - Respects .gitignore (tracked + untracked non-ignored paths via git)
# - Never includes .git (not part of git ls-files output)
# - Omits .gitignore from the archive

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
  echo "Error: run this from a git checkout (this directory must be a git repository)." >&2
  exit 1
fi

out_zip="${script_dir}/fde_interview_packet.zip"
rm -f "$out_zip"

files=()
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  [[ "$path" == .gitignore ]] && continue
  files+=("$path")
done < <(git ls-files -co --exclude-standard)

if [[ ${#files[@]} -eq 0 ]]; then
  echo "Error: no files to package." >&2
  exit 1
fi

zip -q "$out_zip" -- "${files[@]}"
echo "Wrote ${out_zip}"
