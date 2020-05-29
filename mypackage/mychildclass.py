from .myclass import ClassExample

"""
from mypackage.mychildclass import ChildClassExample
x = ChildClassExample("hello", "world")
"""

class ChildClassExample(ClassExample):
	def __init__(self, message, childmessage):
		ClassExample.__init__(self, message)
		self.childmessage = childmessage