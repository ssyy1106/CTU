class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age
    def say(self):
        print(f"Hi, My name is {self.name}, I'm {self.age}")

p = Person("Alice", 30)
print(f"p: {p.__dict__}")

print(f"class person: {Person.__dict__}")