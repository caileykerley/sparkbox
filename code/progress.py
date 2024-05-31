hr = 60

done = 12 + 44 + 24 + 17 + (3*hr) + 33 + 50 + 14 + 25 + 36 + (8+19+12+5+7+17+7+3+1+10+23+8+2+8+2+3+3)
minutes = 12 + 44 + 24 + 17 + (3*hr) + 33 + 50 + 14 + 25 + 36 + (3*hr) + 13 + 31 + 27 + 16 + 34 + (7*hr) + 54 + 60 + 50

print(f"Course Time: {minutes/hr} hours")
print(f"Progress: {round(100*done/minutes,1)}%")