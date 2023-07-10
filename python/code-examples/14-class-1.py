# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 11:16:28 2019

@author: Kanakaraju
"""

class Dog:
    # The init method is called to create an object
    # We give default values for the fields if none
    # are provided
    def __init__(self, name="", height=0, weight=0):
        # self allows an object to refer to itself
        self.name = name
        self.height = height
        self.weight = weight

    # Define what happens when the Dog is asked to
    # demonstrate its capabilities
    def run(self):
        print("{} the dog runs".format(self.name))
    def eat(self):
        print("{} the dog eats".format(self.name))
    def bark(self):
        print("{} the dog barks".format(self.name))


def main():
    # Create a new Dog object
    spot = Dog("Spot", 66, 26)
    spot.bark()

main()