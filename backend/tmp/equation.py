

from tabnanny import check


def plus(*args):
    
    def check_type(arg):
        if isinstance(arg, int) or isinstance(arg, float):
            pass
        else:
            raise TypeError('A number is required here, but got {} instead'.format(type(arg)))

    if len(args) != 2:
        raise TypeError('Expected to have two argumens, but got {}'.format(len(args)))
    for i in args:
        check_type(i)
        
    return sum(args)

def get_even(a):
    return a % 2 == 0


        
    
