import time
import copy
import string
import itertools

def filter_into_two(elements, f):
    """Returns a tuple of two lists, the first one if the provided
       function f returns truthy then second one if the function returned
       falsy.
    """
    a = []
    b = []
    for e in elements:
        if f(e):
            a.append(e)
        else:
            b.append(e)
    return (a, b)

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

def dump_hex(s, width=16):
    "Return a string of the given data in hex format and printable strings just like tcpdump"
    out = ''
    for group in grouper(width, s):
        hexes = [('%X' % ord(c)).rjust(2, '0') if c else '  ' for c in group]
        out += ' '.join(hexes)
        out += '  '
        out += ''.join([['.', c][c in string.printable[:-5]] for c in group if c])
        out += '\n'
    return out

def dict_merge(first, second):
    "Returns a copy of the two given dictionaries merged"
    copied = copy.copy(first)
    copied.update(second)
    return copied

# Gratefully stolen from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
class memoized(object):
   """
   Decorator that caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned, and
   not re-evaluated.
   
   This guy works for any method, instance or not.  The cache has its own lifetime.
   """
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      try:
         return self.cache[args]
      except KeyError:
         value = self.func(*args)
         self.cache[args] = value
         return value
      except TypeError:
         # uncachable -- for instance, passing a list as an argument.
         # Better to not cache than to blow up entirely.
         return self.func(*args)
         
   def __repr__(self):
      """Return the function's docstring."""
      return self.func.__doc__
      
   def __get__(self, obj, objtype):
      """Support instance methods."""
      return functools.partial(self.__call__, obj)

