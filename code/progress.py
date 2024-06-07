hr = 60

done = (
        12 + 44 + 24 + 17 + (3*hr + 33) + 50 + 14 + 25 + 36 + (3*hr + 13)
        + 31
)
minutes = (
        12 + 44 + 24 + 17 + (3*hr + 33) + 50 + 14 + 25 + 36 + (3*hr + 13)
        + 31 + 27 + 16 + 34 + (7*hr + 54) + (1*hr + 50)
)

print(f"Course Time: {minutes/hr} hours")
print(f"Completed Time: {round(done/hr,1)} hours")
print(f"Progress: {round(100*done/minutes,1)}%")