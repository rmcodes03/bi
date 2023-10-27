def jaccard_sim(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    intersection = len(set1.intersection(set2))
    print("Intersection: ", intersection)
    union = len(set1) + len(set2) - intersection
    print("Union: ", union)
    if union == 0:
        return 0.0
    return intersection / union

def generate_shingle_word(text, k):
    shingles = []
    words = text.split()
    if len(text)<k:
        return shingles
    for i in range(len(words)- k+1):
        shingle = ' '.join(words[i : i+k])
        shingles.append(shingle)
    return shingles

with open("text1.txt", "r") as file:
    lines = file.readlines()
doc1_content = "".join(lines)

with open("text2.txt", "r") as file:
    lines = file.readlines()
doc2_content = "".join(lines)

k = int(input("Enter the value of k: "))
shingles1 = generate_shingle_word(doc1_content, k)
shingles2 = generate_shingle_word(doc2_content, k)


print("Set A:", set(shingles1))
print("Set B:", set(shingles2))

jsim = jaccard_sim(shingles1, shingles2)
jdis = 1 - jsim

print("Jaccard Similarity: ", jsim)
print("Jaccard Distance: ", jdis)
