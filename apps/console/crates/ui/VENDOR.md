# Vendored source

`crates/ui` is a GPU UI library. It is a copy with no link to its origin: no upstream sync, no shared code with another repository.

## Origin

1. Zed Industries GPUI, Apache-2.0.
2. `longbridge/gpui-fast` at `c58d957`, Apache-2.0. It adds the retained view tree in `src/fast/`.
3. The `crates/ui` crate of the AFFiNE native playground at `1346d6efc`, Apache-2.0. It fuses the GPUI crates into one crate, removes the proc-macro crates and adds the headless platform and the frame audit.

The license text is in [`LICENSE-APACHE`](LICENSE-APACHE).

## Removed for the console

- The Web platform: `src/platform/web`, `shaders_webgl.wgsl`, the texture transport of instance data in the `wgpu` renderer, and every `wasm` code path and dependency.
- The `wgpu` renderer for a window, with its surface and adapter selection. Linux draws only without a window.
- Images and SVG: `src/elements/{svg,img,image_cache}.rs`, `src/svg_renderer.rs`, the asset cache, the image and SVG atlas keys, image decoding, and the `resvg` and `usvg` dependencies. The `image` dependency keeps only the PNG encoder that the headless renderers use.
- The path builder and its tessellation cache: `src/path_builder.rs`, `src/fast/path_cache.rs` and the `lyon` dependency. `Path` and `Window::paint_path` stay; a caller builds a path with `Path::{new, line_to, curve_to}`.
- Transforms: the 2D transform on every primitive, the sprite transform that only SVG used, the local content mask, and the dot-grid background shader, in the Metal and the WGSL shaders.

## Added for the console

- `StatefulInteractiveElement::{track_scroll, overflow_y_scroll, active}` and `ScrollHandle::set_offset`.
- `Styled::{justify_between, justify_end, items_start, items_end, flex_wrap_reverse, max_w, h_full}`.
- `App::set_quit_mode`.

## Platforms

- macOS: AppKit, Metal and CoreText. This is the platform the console runs on. `MetalHeadlessRenderer` draws the pictures for review.
- Linux: `wgpu` and `cosmic-text` without a window. Tests use it for the frame audit.
