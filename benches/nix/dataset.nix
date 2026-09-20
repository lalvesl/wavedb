# The dataset, once, in the one portable form every bulk loader reads.
#
# Emitting it separately is what lets a server-side seed exist before its Rust
# adapter does: the seed's builder needs a TSV and a bulk loader, nothing more.
{
  pkgs,
  benchGen,
  tier,
  sd,
}:
let
  inherit (tier) rows;
in
# Named by the tier's tag, so the revision is part of the store path: bumping
# it in `params.nix` builds a different dataset rather than reusing this one.
pkgs.runCommand "bench-dataset-${tier.tag}" { nativeBuildInputs = [ benchGen ]; } ''
  mkdir -p "$out"
  bench-gen emit-tsv --rows ${rows} --seed ${sd} \
    --out "$out/dataset.tsv"
  cat > "$out/manifest.txt" <<EOF
  tier=${tier.name}
  rows=${rows}
  seed=${sd}
  dataset_revision=${tier.rev}
  columns=id,kind,score,name,tag,body
  EOF
''
