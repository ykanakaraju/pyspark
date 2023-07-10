# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 19:16:22 2019

@author: Kanakaraju
"""

#######################################
#     Inheritance
#######################################

class Polygon:    
    def __init__(self, no_of_sides):
        self.n = no_of_sides
        self.sides = [0 for i in range(no_of_sides)]

    def input_sides(self):
        self.sides = [float(input("Enter side " + str(i+1 )+ " : ")) 
                      for i in range(self.n)]

    def display_sides(self):
        for i in range(self.n):
            print("Side",i+1,"is",self.sides[i])
            
class Triangle(Polygon):
    def __init__(self):
        Polygon.__init__(self,3)

    def find_area(self):
        a, b, c = self.sides
        # calculate the semi-perimeter
        s = (a + b + c) / 2
        area = (s*(s-a)*(s-b)*(s-c)) ** 0.5
        print('The area of the triangle is %0.2f' %area)
        
t = Triangle()
t.input_sides()
t.display_sides()
t.find_area()


#######################################
#    Multiple Inheritance
#######################################
class BaseClass1:
    def __init__(self):
        super(BaseClass1, self).__init__()
        print("BaseClass1")
        
class BaseClass2:
    def __init__(self):
        super(BaseClass2, self).__init__()
        print("BaseClass2")
        
class Derived(BaseClass2, BaseClass1):
    def __init__(self):
        super(Derived, self).__init__()
        print("Derived")

derived = Derived() 


#######################################
#    Multilevel Inheritance
#######################################

class animal:
    def do(self):
        print("I can eat")
        
class person(animal):
    def do(self):
        super().do()
        print("I can think")  
        
class woman(person):
     def do(self):
        super().do()
        print("I can give birth to a baby")   
        
class mother(woman):        
    def do(self):
        super().do()
        print("I gave birth to a baby")
        
class grandmother(mother):        
    def do(self):
        super().do()
        print("My daughter gave birth to a baby")
        
a = grandmother()
a.do()  

#######################################
#    Method Overriding
#######################################
class Rectangle:
    def __init__(self, length, breadth):
        self.length = length
        self.breadth = breadth
        
    def get_area(self):
        return self.length * self.breadth

class Square(Rectangle):
    def __init__(self, side):
        self.side = side
        Rectangle.__init__(self, side, side)
        
    def get_area(self):
        return self.length * self.breadth    

s = Square(4)
print("Area of Square: ", s.get_area())

r = Rectangle(3, 4)
print("Area of Rectangle: ", r.get_area())

#######################################
#    Getters and Setters
#######################################
class GetSet:
    def __init__(self, name):
        self.name = name
        
    def set_name(self, name):
        self.name = name
        
    def get_name(self):
        return self.name
    
obj = GetSet("Bill Gates")
print(obj.get_name())

obj.set_name("Jeff Bezos")
print(obj.get_name())
        

       