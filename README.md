string = "apple, orange, banana, strawberry, blueberry"
fruits = string.split(", ")
fruits = fruits[:-2]
new_string = ", ".join(fruits)
print(new_string) # "apple, orange, banana"
