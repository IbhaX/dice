import re
import nltk
import json
import pandas as pd
nltk.download('punkt')  # Required for word tokenization
from nltk.tokenize import word_tokenize

class JobTitleCleaner:
    def __init__(self):
        pass

    def clean_title(self, title: str) -> str:
        # Remove salary information like "$2,220 per week", "$1,941-2,236 per week"
        title = re.sub(r'\$\d{1,3}(,\d{3})*(\s*-\s*\$?\d{1,3}(,\d{3})*)?\s*(per\s*\w+)?', '', title)

        # Remove standalone numbers that could represent salary ranges (e.g., "1,558-1,795 per week")
        title = re.sub(r'\d{1,3}(,\d{3})?(\s*-\s*\d{1,3}(,\d{3})*)?\s*(per\s*\w+)?', '', title)

        # Remove state abbreviations (e.g., "- TX", "- NY", etc.)
        title = re.sub(r'\s*-\s*[A-Za-z]{1,2}(,)?(\s[A-Za-z]{1,2})?$', '', title)

        # Remove any location information that comes after a dash (e.g., "- Williston, T")
        title = re.sub(r'\s*-\s*[A-Za-z\s,]+$', '', title)

        # Tokenize and reconstruct the title using NLTK
        tokens = word_tokenize(title)
        clean_title = ' '.join(tokens)

        return clean_title.strip()

# Test cases
titles = [
    "Travel Skilled Nursing Facility Physical Therapist - $2,220 per week",
    "Restaurant Manager",
    "Travel Home Health Occupational Therapist - $1,941-2,236 per week",
    "A Data Trainer",
    "Mathematics Expertise Sought for A Training",
    "Travel Physical Therapist - $1,558-1,795 per week",
    "Document Control Clerk - Williston, T",
    "Civil Engineer In"
]

cleaner = JobTitleCleaner()


with open("out.json") as f:
    items = json.load(f)

for item in items:
    if len(item["Title"].split()) > 4:
        item["Title"] = cleaner.clean_title(item["Title"])
    
    if item["SalaryFrom"] == -1:
        item["SalaryFrom"] = ""
    
    if item["SalaryUpto"] == -1:
        item["SalaryUpto"] = ""
    
    if item["Experience"] > 15 or item["Experience"] == 0:
        item["Experience"] = ""
        
        


df = pd.DataFrame(items)
df.to_excel("cleaned.xlsx", index=False)
