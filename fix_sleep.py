import re

path = '/home/akhil/xv6-riscv/user/prodcons.c'
content = open(path).read()

# Replace all sleep(delay) with uptime-based busy wait
# This handles both producer and consumer sleep calls
content = content.replace(
    '    if (delay)\n      sleep(delay);',
    '    if (delay) {\n      int tw = uptime();\n      while (uptime() - tw < delay);\n    }'
)

open(path, 'w').write(content)
print("Fixed! sleep() replaced with uptime-based busy wait.")
print("Occurrences of 'sleep' remaining:", content.count('sleep('))
