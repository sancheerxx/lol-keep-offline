# -*- coding: utf-8 -*-
"""生成一个简单的应用图标（不依赖 Pillow）：深色圆角底 + 蓝色圆点。"""
import math
import struct

SIZE = 64
BG = (0x14, 0x16, 0x1D)
PANEL = (0x1C, 0x1F, 0x28)
BLUE = (0x4C, 0x8D, 0xFF)
RING = (0x2E, 0x34, 0x41)


def blend(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def pixel(x, y):
    """返回 (B, G, R, A)。"""
    cx = cy = SIZE / 2 - 0.5
    dx, dy = x - cx, y - cy
    r = math.hypot(dx, dy)
    radius = SIZE / 2 - 2

    # 圆角外透明
    if r > radius:
        return (0, 0, 0, 0)

    color = PANEL
    # 内层小圆（状态点）
    inner = radius * 0.42
    if r < inner:
        color = BLUE
    elif r < inner + 2:
        color = blend(BLUE, PANEL, (r - inner) / 2)
    elif r < inner + 4:
        color = blend(PANEL, RING, 0.7)

    # 外圈柔和渐变
    if r > radius - 3:
        color = blend(color, BG, (r - (radius - 3)) / 3)

    return (color[2], color[1], color[0], 255)


def main():
    rows = []
    for y in range(SIZE - 1, -1, -1):  # ICO 存的是自下而上的行
        row = b""
        for x in range(SIZE):
            row += bytes(pixel(x, y))
        rows.append(row)
    body = b"".join(rows)

    # BITMAPINFOHEADER（高度写成 2 倍，因为含 AND 掩码）
    header = struct.pack(
        "<IiiHHIIiiII", 40, SIZE, SIZE * 2, 1, 32, 0,
        len(body), 0, 0, 0, 0)
    # AND 掩码：全 0 表示全部不透明
    mask = b"\x00" * (((SIZE + 31) // 32) * 4 * SIZE)
    image = header + body + mask

    # ICO 头 + 目录项
    out = struct.pack("<HHH", 0, 1, 1)
    out += struct.pack("<BBBBHHII", SIZE, SIZE, 0, 0, 1, 32, len(image), 22)
    out += image

    with open("app.ico", "wb") as f:
        f.write(out)
    print("app.ico written", len(out), "bytes")


main()
