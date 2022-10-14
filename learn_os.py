import os
import tempfile
import time

def advanced_python_function_parameters(normal_param, normal_param_2,  *arguments):
   print(f"The normal parameter is: {normal_param}")
   print(f"arguments is:")
   for arg in arguments:
       print(arg)
   print("end argument")
   print(f"keyword is:")
   for kw in keywords:
       print(kw, ":", keywords[kw])
   print("end keyword")

a = ("It's really very, VERY runny, sir.")
print(*a)
advanced_python_function_parameters(
  normal_param = "Limburger",
  normal_param_2="It's very runny, sir.",
  *a,
  shopkeeper="Michael Palin",
  client="John Cleese",
  sketch="Cheese Shop Sketch"
)


a = tempfile.NamedTemporaryFile(dir=".", mode="wb")
print(a.name)
a.write("1\n".encode("utf-8"))
a.write("2\n".encode("utf-8"))
print(os.path.exists(a.name))
