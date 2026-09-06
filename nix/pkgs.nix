# The one `nixpkgs` instantiation every other module in this repo receives.
#
# It exists as a module rather than a `let` binding because the unfree
# predicate below is a policy decision, and a policy that lives in two places
# is a policy that will disagree with itself.
{
  nixpkgs,
  system,
  rust-overlay,
}:
import nixpkgs {
  inherit system;
  overlays = [ (import rust-overlay) ];
  config = {
    # MongoDB is SSPL (`meta.unfree = true`) and the benchmark's reference
    # peer (RFC 0060). Scoped to that one package on purpose: a blanket
    # `allowUnfree` would silently license anything a future dependency drags
    # in.
    #
    # `mongodb-ce`, not `mongodb`: unfree packages are not distributed by
    # cache.nixos.org, so the source attribute compiles the whole server
    # locally — hours of C++, more than the rest of the suite combined,
    # repeated at every version bump. `-ce` is the official prebuilt tarball
    # plus `autoPatchelf`, so the licence costs a download instead. It ships
    # `mongod`/`mongos` only; `mongosh` and `mongoimport` come from their own
    # (free) packages.
    allowUnfreePredicate = pkg: builtins.elem (nixpkgs.lib.getName pkg) [ "mongodb-ce" ];
  };
}
