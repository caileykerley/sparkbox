done = 12 + 44 + 24 + 17 + (3*60) + 33 + (8 + 7 + 2 + 3)
minutes = 12 + 44 + 24 + 17 + (3*60) + 33 + 50 + 14 + 25 + 36 + (3*60) + 13 + 31 + 27 + 16 + 34 + (7*60) + 54 + 60 + 50

print(f"Course Time: {minutes/60} hours")
print(f"Progress: {round(100*done/minutes,1)}%")