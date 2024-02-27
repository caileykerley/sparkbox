done = 12 + 44 + 24 + 17 + (30+2+16+14+16+6+10+23+7+4+6+19+10+7)
minutes = 12 + 44 + 24 + 17 + (3*60) + 33 + 50 + 14 + 25 + 36 + (3*60) + 13 + 31 + 27 + 16 + 34 + (7*60) + 54 + 60 + 50

print(f"Course Time: {minutes/60} hours")
print(f"Progress: {round(100*done/minutes,1)}%")