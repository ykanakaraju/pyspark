# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 11:16:28 2019

@author: Kanakaraju
"""

class Square:
    def __init__(self, height="0", width="0"):
        self.height = height
        self.width = width

    # This is the getter
    # @property defines that any call to height runs the code in the height method below it
    @property
    def height(self):
        print("Retrieving the height")

        # Put a __ before this private field
        return self.__height

    # This is the setter
    @height.setter
    def height(self, value):
        # We protect the height from receiving a bad value
        if value.isdigit():
            # Put a __ before this private field
            self.__height = value
        else:
            print("Please only enter numbers for height")

    # This is the getter
    @property
    def width(self):
        print("Retrieving the width")
        return self.__width

    # This is the setter
    @width.setter
    def width(self, value):
        if value.isdigit():
            self.__width = value
        else:
            print("Please only enter numbers for width")

    def get_area(self):
        return int(self.__width) * int(self.__height)

def main():
    square = Square()
    height = input("Enter height : ")
    width = input("Enter width : ")
    square.height = height
    square.width = width
    print("Height :", square.height)
    print("Width :", square.width)
    print("The Area is :", square.get_area())

main()