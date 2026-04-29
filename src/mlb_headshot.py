"""
MLB silo headshot background cleanup for card PNGs.

CDN assets often use green/teal studio backdrops; newer photos also use black or
charcoal. Vectorized green masking alone misses dark neutral backgrounds.
"""

from __future__ import annotations

import numpy as np
from PIL import Image, ImageDraw


def prepare_mlb_headshot_rgb(img: Image.Image) -> Image.Image:
    """Flatten RGBA onto white so transparent pixels are not composited as black."""
    if img.mode == "RGBA":
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[3])
        return bg
    return img.convert("RGB")


def _pixel_is_studio_backdrop(r: int, g: int, b: int) -> bool:
    """True for classic green screen or dark neutral (black/charcoal) silo backdrops."""
    # Green/teal — matches mallitalytics_daily_card legacy heuristic
    if (g > r) and (g > b) and (g > 80) and (abs(int(g) - r) + abs(int(g) - b) > 40):
        return True
    # Dark neutral: black / dark gray JPEG background (not strongly chromatic)
    mx, mn = max(r, g, b), min(r, g, b)
    if mx - mn <= 42 and (r + g + b) / 3.0 < 72:
        return True
    return False


def neutralize_mlb_headshot_background(
    img: Image.Image, replace_rgb: tuple[int, int, int] = (255, 255, 255)
) -> Image.Image:
    """
    Replace MLB studio backdrops with a neutral card color.

    1. Composite RGBA onto white (avoids black fringes from alpha).
    2. Mask obvious green/teal pixels (whole-image, fast).
    3. Flood-fill from edge seeds where the pixel still looks like backdrop
       (covers black/charcoal regions connected to the border).
    """
    im = prepare_mlb_headshot_rgb(img)
    arr = np.array(im, dtype=np.uint8)
    r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
    green_bg = (g > r) & (g > b) & (g > 80) & (
        np.abs(g.astype(int) - r) + np.abs(g.astype(int) - b) > 40
    )
    arr[green_bg, 0] = replace_rgb[0]
    arr[green_bg, 1] = replace_rgb[1]
    arr[green_bg, 2] = replace_rgb[2]
    im = Image.fromarray(arr, mode="RGB")

    w, h = im.size
    seeds: list[tuple[int, int]] = [
        (0, 0),
        (w - 1, 0),
        (0, h - 1),
        (w - 1, h - 1),
        (w // 2, 0),
        (w // 2, h - 1),
        (0, h // 2),
        (w - 1, h // 2),
    ]
    for xy in seeds:
        try:
            p = im.getpixel(xy)
            if isinstance(p, tuple) and len(p) >= 3:
                pr, pg, pb = p[0], p[1], p[2]
            else:
                continue
            if _pixel_is_studio_backdrop(pr, pg, pb):
                ImageDraw.floodfill(im, xy, replace_rgb, thresh=40)
        except (ValueError, OSError, IndexError, TypeError):
            continue

    return im
