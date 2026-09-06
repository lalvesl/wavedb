# Runtime libraries for `wavedb-monitor-gui`.
#
# eframe/winit link these dynamically — without them the binary panics with
# `NoWaylandLib`. Shared between the dev shell's `LD_LIBRARY_PATH` and the
# GUI apps, which is why it is a list here and not an inline `with pkgs`.
# Mirrors the sibling egui_shadcn flake's `nativeLibs`.
pkgs: with pkgs; [
  libxkbcommon
  libGL
  wayland
  libx11
  libxcursor
  libxrandr
  libxi
  fontconfig
]
