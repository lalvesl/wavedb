# The dataset, once, in the one portable form every bulk loader reads.
#
# Emitting it separately is what lets a server-side seed exist before its Rust
# adapter does: the seed's builder needs a TSV and a bulk loader, nothing more.
{
  pkgs,
  benchGen,
  rows,
  sd,
}:
pkgs.runCommand "bench-dataset-${rows}" { nativeBuildInputs = [ benchGen ]; } ''
  mkdir -p "$out"
  bench-gen emit-tsv --rows ${rows} --seed ${sd} \
    --out "$out/dataset.tsv"
  cat > "$out/manifest.txt" <<EOF
  rows=${rows}
  seed=${sd}
  columns=id,kind,score,name,tag,body
  EOF
''
