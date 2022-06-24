import math

level_map = {
    range(10, int(math.inf - 1)): "level 1",
    range(9, 10): "level 2",
}

src_value = 10
for range_key in level_map:
    if src_value in range_key:
        print(level_map[range_key])
        break
