for i in range(1, 5):
    print(i)

try:
    for i in range(0, 35.3):
        print(i)
except Exception as e:
    print(e)  # 'float' object cannot be interpreted as an integer
