import re
text = "Hello, 世界!"
clean_text = re.sub(r'[^\\x00-\\x7F]+', '', text)
print(clean_text)
