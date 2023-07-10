# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 11:16:28 2019

@author: Kanakaraju
"""

class Number():
    pass               # pass is a place holder code for nothing (or perhaps a TODO)
    
a = Number             # this is a class 
b = Number()           # creates an object
print(a, b)

########################################################
# self : a pointer to the object in consideration
# cls  : a pointer to the class from which object is derived
########################################################

class SomeClass():
    def Hello(self):
        print("Good Morning")
        
obj = SomeClass()
obj.Hello()

########################################################
# Built-in Class Attributes 
########################################################

class BltInAttribs():
    emp_count = 0
    
print("BltInAttribs.__dict__  >> ", BltInAttribs.__dict__)
print("BltInAttribs.__name__  >> ", BltInAttribs.__name__)
print("BltInAttribs.__doc__  >> ", BltInAttribs.__doc__)
print("BltInAttribs.__module__  >> ", BltInAttribs.__module__)
print("BltInAttribs.__bases__  >> ", BltInAttribs.__bases__)


########################################################
# Constructor and Destructor
########################################################
class ConstructDestruct:
    account_type = 0
    
     # constructor
    def __init__(self, acct_id, name, password):        
       # these are container attributes 
       self.id = acct_id
       self.name = name
       self.__password = password
    
       print("Constructing the object")
       
    def hello(self):
        print("Hello {}".format(self.name))
    
    # destructor
    def __del__(self):
        print("Good Bye {}".format(self.name))
    
obj = ConstructDestruct(10, 'spyder', 'python')
print(obj.__dict__)
obj.hello()
del obj

########################################################
# Access Specifiers: Public, Protected, Private
########################################################
class AccessEx:      
    def __init__(self):
        self.publc = "Public Attribute"
        self._prtcd = "Protected Attribute"
        self.__privt = "Private Attribute"
        
myObj = AccessEx()
print (myObj.publc)
print (myObj._prtcd)
print (myObj.__privt)

########################################################
# Class Methods (static) and Instance Methods
# Access Specifiers: Public, Protected, Private
########################################################
class Account:    
    account_type = 'Savings'    # public attribute
    _balance = 10000            # protected attribute. prefixed with _
    __loan_amount = 100000      # private attribute. prefixed with __
           
    @classmethod
    def cls_method(cls, prefix):
        print(prefix, cls.account_type)
        
    #instance method
    def welcome(self):
        print('Welcome')
        
    def set_account_type(self, account_type):
        self.account_type = account_type
        
    def get_welcome_message(self):
        if self.account_type == 'Savings' :
            return 'Welcome. We have exciting offers for Savings Account holder.'
        else:
            return 'Welcome to XYZ Bank'
        
    def _protectedMethod(self, name):
        print('protected method', name)  
    
    # private method is not acceessible outside of the class    
    def __privateMthd(self, name):
        print('protected method', name)
        
account = Account()
print(account.get_welcome_message())


Account.cls_method(1)

account.set_account_type("Current")

Account.cls_method(2)
account.cls_method(3)

print(account.account_type)
account.welcome()

#account.__privateMthd('raju')  # not accessible via an object

# But, even private methods can be access by prefixing them with _<classname>
account._Account__privateMthd('raju')    