# -*- coding: utf-8 -*-


import random

#produces a random number between 1 and 50 (inclusive)
ran_int = random.randint(1, 50)
print(ran_int)

#produces a random decimal between 0 and 1
ran_f = random.random()
print(ran_f)

#produce only even numbers. numers are only produced from the range
rand_num = random.randrange(0, 51, 2)
print("The random value is " , rand_num)


