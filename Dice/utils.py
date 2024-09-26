import re
import spacy
from spacy.matcher import PhraseMatcher, Matcher
from typing import List, Tuple
from autocorrect import Speller
import requests
import json

def get_ai_response(user_message):
  response = requests.post(
      url="https://openrouter.ai/api/v1/chat/completions",
      headers={
          "Authorization": f"Bearer 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"
      },
      data=json.dumps({
          "model": "meta-llama/llama-3.1-8b-instruct:free",
          "messages": [
              {"role": "user", "content": user_message + "\n\nFind the required years of experience for the job Expected output: one number experience required example: '5+"}
          ],
          "top_p": 1,
          "temperature": 0.95,
          "frequency_penalty": 0,
          "presence_penalty": 0,
          "repetition_penalty": 1,
          "top_k": 0,
      })
  )
  return response.json()["choices"][0]["message"]["content"]


def foramt_salary(value, _type):
    _type = _type.lower()
    value = value.lower()
    if not str(value).strip(): return -1, ''
    
    value = value.split('/')[0]
    value = value.split('per')[0]
    value = value.replace('usd','').replace('$', '').replace(',', '').strip()

    if value.endswith('k'):
        try:
            value = float(value.replace('k','').strip()) * 1000
        except:
            return -1, ''
    try:
        value = float(value)
    except:
        return -1, ''
    
    if _type:
        if _type == 'hourly' or _type == 'hr' or _type == 'hour':
            return [round(value * 2080), 'Year']
        
        elif _type == 'daily' or _type == 'day':
            return [round(value * 260), 'Year']
        
        elif _type == 'weekly' or _type == 'weekly' or _type == 'week':
            return [round(value * 52), 'Year']
        
        elif _type == 'monthly' or _type == 'month':
            return [round(value * 12), 'Year']
        
        elif _type == 'yearly' or _type == 'year' or _type == 'annual' or _type == 'annually':
            return [round(value), 'Year']
    else:
        if value <= 200:
            # consider less than 200 as hourly pay
            return [round(value * 2080), 'Year']
        elif value <= 500:
            # consider less than 500 as daily pay
            return [round(value * 260), 'Year']
        elif value <= 2000:
            return [round(value * 52), 'Year']

        elif value <= 10000:
            return [round(value * 12), 'Year']

        elif value > 10000:
            return [round(value), 'Year']

    return [round(value), _type] 



def validate(record, keys, default=''):
    for key in keys:
        if key in record:
            record = record[key]
        else:
            return default
    return record



def find_payout_term(text):
    text = text.lower()
    if 'hr' in text or 'hour' in text:
        return 'Hour'
    elif 'day' in text or 'daily' in text:
        return 'Day'
    elif 'week' in text or 'weekly' in text:
        return 'Week'
    elif 'month' in text or 'monthly' in text:
        return 'Month'
    elif 'year' in text or 'yearly' in text or 'annual' in text or 'annually' in text:
        return 'Year'
    return ''


class NLPUtils:
    def __init__(self, logger):
        self.logger = logger
        self.nlp = self._load_spacy_model()
        self.job_keywords = set(['engineer', 'manager', 'developer', 'analyst', 'specialist', 'coordinator', 'assistant', 'director', 'supervisor', 'technician', 'expert', 'expertise', 'experts'])
        self.languages = set(["python", "java", "c++", "javascript", "ruby", "php", "swift", "go", "rust", "typescript", "kotlin", "scala", "matlab", "perl", "sql", "html", "css"])
        self.spell = Speller(lang='en')
        
    @staticmethod
    def _load_spacy_model():
        try:
            return spacy.load("en_core_web_sm")
        except OSError:
            print("Downloading the 'en_core_web_sm' model...")
            spacy.cli.download("en_core_web_sm")
            return spacy.load("en_core_web_sm")

    def extract_job_title(self, text: str) -> str:
        doc = self.nlp(text)
        for chunk in doc.noun_chunks:
            if self._is_job_title(chunk.text):
                clean_title = self._clean_title(chunk.text)
                languages_in_title = self._find_languages_in_title(text)
                if languages_in_title:
                    clean_title += f" ({'/'.join(languages_in_title)})"
                return clean_title
        return ""

    def _is_job_title(self, text: str) -> bool:
        return any(keyword in text.lower() for keyword in self.job_keywords)

    @staticmethod
    def _clean_title(title: str) -> str:
        return title.strip('() \n\t').removeprefix("- ")

    def _find_languages_in_title(self, text: str) -> List[str]:
        return [lang for lang in self.languages if lang.lower() in text.lower()]

    def spellcheck(self, text: str) -> str:
        try:
            return ' '.join(self.spell(word) for word in text.split())
        except Exception as e:
            self.logger.error(f"Not spell checked: {str(e)}")
            return text
    
    @staticmethod
    def determine_education(job_description):
        education_keywords = {
            "High School": ["high school", "ged"],
            "Associate": ["associate", "associate's degree"],
            "Bachelor's Degree": ["bachelor", "bachelor's degree", "bachelors", "undergraduate", "b.s", "b.s.", "bs", "bs in", "bs or", "degree", "degree in", "college degree", "technical degree"],
            "Master's Degree": ["master", "master's degree", "masters", "m.s", "m.s.", "bs/ms", "ms in", "ms or", "ms degree"],
            "PhD": ["phd", "doctorate", "doctoral", "ph.d", "postgraduate", "postgraduation"]
        }

        job_description_lower = job_description.lower()
        education_order = ["High School", "Associate", "Bachelor's Degree", "Master's Degree", "PhD"]

        for edu_level in reversed(education_order):
            if any(keyword in job_description_lower for keyword in education_keywords[edu_level]):
                return edu_level

        return "Not Mentioned"

    def determine_job_type(self, job_description):
        doc = self.nlp(job_description.lower())
        job_types = {
            "Full Time": ["full-time", "full time", "permanent", "regular"],
            "Part Time": ["part-time", "part time"],
            "Contract": ["contract", "temporary", "fixed term"]
        }

        for token in doc:
            for job_type, keywords in job_types.items():
                if token.text in keywords:
                    return job_type

        return "Unknown"
    
    def find_industry_type(self, job_description):
        industry_keywords = [
            "technology", "finance", "healthcare", "education", "retail",
            "manufacturing", "consulting", "agriculture", "automotive",
            "construction", "energy", "entertainment", "fashion",
            "food and beverage", "hospitality", "insurance", "legal",
            "logistics", "media", "mining", "pharmaceuticals",
            "real estate", "telecommunications", "transportation",
            "aerospace", "biotechnology", "chemicals", "e-commerce",
            "environmental services", "sports", "Information Technology"
        ]

        matcher = PhraseMatcher(self.nlp.vocab)
        patterns = [self.nlp(text) for text in industry_keywords]
        matcher.add("INDUSTRY", patterns)
        doc = self.nlp(job_description)
        matches = matcher(doc)
        
        industries = set(doc[start:end].text for _, start, end in matches)
        
        return ",".join(industries) if industries else "Information Technology"

    def find_years_of_experience(self, text):
        matcher = Matcher(self.nlp.vocab)
        patterns = [
            [{"LOWER": {"REGEX": r"\b\d+\+?\b"}}, {"LOWER": {"IN": ["years", "year"]}}, {"LOWER": {"IN": ["hands-on", "experience"]}, "OP": "?"}],
            [{"LOWER": {"IN": ["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"]}},
            {"LOWER": {"IN": ["years", "year"]}}, {"LOWER": {"IN": ["of", "hands-on", "experience"]}, "OP": "?"}],
            [{"LOWER": "at"}, {"LOWER": "least"}, {"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": {"IN": ["years", "year"]}}],
            [{"LOWER": "no"}, {"LOWER": "more"}, {"LOWER": "than"}, {"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": {"IN": ["years", "year"]}}],
            [{"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": "to"}, {"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": {"IN": ["years", "year"]}}],
            [{"LOWER": "between"}, {"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": "and"}, {"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": {"IN": ["years", "year"]}}],
            [{"LOWER": {"REGEX": r"\b\d+\+?\b"}}, {"LOWER": {"IN": ["years", "year"]}}, {"LOWER": {"IN": ["experience", "in"]}}],
            [{"LOWER": "more"}, {"LOWER": "than"}, {"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": {"IN": ["years", "year"]}}],
            [{"LOWER": {"REGEX": r"\b\d+\b"}}, {"LOWER": "years"}, {"LOWER": "experience"}, {"LOWER": "required"}]
        ]

        matcher.add("EXPERIENCE_YEARS", patterns)
        doc = self.nlp(text)
        matches = matcher(doc)

        max_years = 0
        for _, start, end in matches:
            years = re.findall(r'\d+', doc[start:end].text)
            if years:
                max_years = max(max_years, int(years[-1]))

        return max_years

    def process_job_description(self, job_description: str) -> Tuple[str, str, str, str, str, int]:
        spellchecked_description = self.spellcheck(job_description)
        education = self.determine_education(job_description)
        job_type = self.determine_job_type(job_description)
        industry_type = self.find_industry_type(job_description)
        years_of_experience = self.find_years_of_experience(job_description)
        
        return (spellchecked_description, education, job_type, industry_type, years_of_experience)