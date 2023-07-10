
class Sum:    
    @staticmethod
    def get_sum(*args):        
        s = 0
        for i in args:
            s += i
        return s
    
def main():
    print("Sum: ", Sum.get_sum(1,2,3))

main()