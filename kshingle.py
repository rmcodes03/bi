def generate_shingle_character(text, k):
    shingles = []
    if len(text)<k:
        return shingles
    for i in range(len(text)- k+1):
        shingle = ''.join(text[i : i+k])
        shingles.append(shingle)
    return set(shingles)

k = int(input("Enter value of k: "))
with open("test.txt", "r") as file:
    lines = file.readlines()
text = "".join(lines)

print(generate_shingle_character(text, k))
