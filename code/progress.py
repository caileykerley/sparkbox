hr = 60

done = (
        12 + 44 + 24 + 17 + (3*hr + 33) + 50 + 14 + 25 + 36 + (3*hr + 13)
        + 31 + 27 + 16 + 34 + (7*hr + 54) + (1*hr + 50)
)
course_time = (
        12 + 44 + 24 + 17 + (3*hr + 33) + 50 + 14 + 25 + 36 + (3*hr + 13)
        + 31 + 27 + 16 + 34 + (7*hr + 54) + (1*hr + 50)
)

print(f"Course Time: {course_time/hr} hours")
print(f"Completed Time: {round(done/hr,1)} hours")
print(f"Progress: {round(100*done/course_time,1)}%")
print("-------")
print(f"Remaining Time: {round((course_time-done)/hr,1)} hours")