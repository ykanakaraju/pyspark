
# display all magic methods in 'int' class
dir(int)

# Example 1
class Student:   
    
    def __init__(self, roll_num=0, name="", age=0):
        self.roll_num = roll_num
        self.name = name
        self.age = age        
       
    def __str__(self):
        # :02d adds a leading zero to have a minimum of 2 digits
        return "RNo: {}, Name:{}, Age:{}".format(self.roll_num,self.name, self.age)    
   
# Example 2
class Employee:
    
   def __new__(cls):
       print ("__new__ magic method is called")
       inst = object.__new__(cls)
       return inst
    
   def __init__(self):
        print ("__init__ magic method is called")
        self.name = 'Raju'


def main():
    print("========  Student class =============")
    s1 = Student(5, "Aditya", 15)
    s2 = Student(6, "Amrita", 13)
    print(s1)
    print(s2)  
    
    print("========  Employee class =============")
    e1 = Employee()
       
main()