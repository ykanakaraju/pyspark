# -*- coding: utf-8 -*-

#================================================
#     enumerate
#================================================
# The enumerate() method adds counter to an iterable
# and returns it (the enumerate object).
#================================================

fruits = ['apple', 'banana', 'mango']
enumFruits = enumerate(fruits)

print(type(enumFruits))

# converting to list
print(list(enumFruits))

# changing the default counter
enumFruits = enumerate(fruits, 5)
print(list(enumFruits))

#================================================
#     filter
#================================================
# filter() method constructs an iterator from elements 
# of an iterable for which a function returns true.
#================================================

alphabets = ['a', 'b', 'd', 'e', 'i', 'j', 'o']

def filterVowels(alphabet):
    return (alphabet in ['a', 'e', 'i', 'o', 'u'])


filteredVowels = filter(filterVowels, alphabets)

print('The filtered vowels are:')
for vowel in filteredVowels:
    print(vowel)
    