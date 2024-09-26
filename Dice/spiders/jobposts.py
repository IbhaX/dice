from typing import Iterable
import scrapy
from scrapy.http import Request
from urllib.parse import urlencode
import json
import datetime
from uuid import uuid4
from ..utils import *
import json

def hash_dict(item):
    dict_str = json.dumps(item, sort_keys=True)
    return hash(dict_str)

today = datetime.datetime.today().strftime('%Y-%m-%d')


class JobpostsSpider(scrapy.Spider):
    name = "jobposts"
    
    def __init__(self):
        self.nlp = NLPUtils(self.logger)
        self.unique_items = set()

    def start_requests(self) -> Iterable[Request]:
        locations = [
                        ('NY', '43.2994285', '-74.21793260000001', 'State'),
                        ('CA', '36.778261', '-119.4179324', 'State'),
                        ('WA', '47.7510741', '-120.7401386', 'State'),
                        ('CO', '39.5500507', '-105.7820674', 'State'),
                        ('HI', '19.8986819', '-155.6658568', 'State'),
                        ('VT', '44.5588028', '-72.57784149999999', 'State'),
                        ('DC', '38.9059849', '-77.03341790000002', 'State'),
                        ('NJ', '40.7177545', '-74.0431435', 'City')
                     ]
        for state in locations[:]:
            params = {
                        'locationPrecision': state[3],
                        'adminDistrictCode2': state[0],
                        'latitude': state[1],
                        'longitude': state[2],
                        'countryCode2': 'US',
                        'radius': '30',
                        'radiusUnit': 'mi',
                        'page': '1',
                        'pageSize': '1000',
                        'searchId': str(uuid4()),
                        'facets': 'employmentType|postedDate|workFromHomeAvailability|employerType|easyApply|isRemote',
                        'filters.employmentType': 'FULLTIME',
                        # 'filters.postedDate': 'SEVEN',
                        'fields': 'id|jobId|guid|summary|title|postedDate|modifiedDate|jobLocation.displayName|detailsPageUrl|salary|clientBrandId|companyPageUrl|companyLogoUrl|positionId|companyName|employmentType|isHighlighted|score|easyApply|employerType|workFromHomeAvailability|isRemote|debug',
                        'culture': 'en',
                        'recommendations': 'true',
                        'interactionId': '1',
                        'fj': 'true',
                        'includeRemote': 'true',
                    }
            headers = {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                        'x-api-key': '1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8',
                    }
            url = 'https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search?' + urlencode(params)
            yield scrapy.Request(url, headers=headers, meta={'state':state})


    def parse(self, response):
        data = json.loads(response.text)       
        
        for record in data['data']:
            item = {}
            title = self.nlp.extract_job_title(validate(record, ['title']))
            item['Domain'] = 'www.dice.com'
            item['PostUrl'] = record['detailsPageUrl']
            item['JobID'] = record['id']
            title = title if title else validate(record, ['title'])
            item['Title'] = self.nlp.spellcheck(title)
            location = record['jobLocation']['displayName']
            location = location.split(',')
            item['City'] = location[0].strip()
            item['State'] = location[1].strip()

            item['Speciality'] = ''
            item['JobType'] = validate(record, ['employmentType'])
            
            item['JobDetails'] = self.nlp.spellcheck(validate(record, ['summary']))
            
            item['Company'] = validate(record, ['companyName'])
            
            item['PostedOn'] = record['postedDate'].split('T')[0]
            
            item['SalaryFrom'] = ''
            item['SalaryUpto'] = ''
            item['PayoutTerm'] = ''

            salary = validate(record, ['salary'])
            if not salary:
                continue
            
            item['SalaryTotal'] = salary
            if salary:
                payout = find_payout_term(salary)
                salary = salary.split('-')

                item['SalaryFrom'], pay_out = foramt_salary(salary[0], payout)
                item['SalaryUpto'], pay_out = foramt_salary(salary[-1], payout)
                if item['SalaryFrom'] <= 0:
                    item['SalaryFrom'] = item['SalaryUpto']
                item['PayoutTerm'] = pay_out

            
            item['IsEstimatedSalary'] = ''
            item['ScrapedOn'] = today

            yield scrapy.Request(record['detailsPageUrl'], callback=self.parse_details, cb_kwargs={'item':item})
            
            # if item['SalaryFrom'] and item['SalaryFrom'] > 0:
            #     yield item

        # print(data['meta'])
        if data['meta']['currentPage'] < data['meta']['pageCount']:
            state = response.meta['state']
            params = {
                            'locationPrecision': 'State',
                            'adminDistrictCode2': state[0],
                            'latitude': state[1],
                            'longitude': state[2],
                            'countryCode2': 'US',
                            'radius': '30',
                            'radiusUnit': 'mi',
                            'page': str(data['meta']['currentPage']+1),
                            'pageSize': '1000',
                            'searchId': str(uuid4()),
                            'facets': 'employmentType|postedDate|workFromHomeAvailability|employerType|easyApply|isRemote',
                            'filters.employmentType': 'FULLTIME',
                            # 'filters.postedDate': 'SEVEN',
                            'fields': 'id|jobId|guid|summary|title|postedDate|modifiedDate|jobLocation.displayName|detailsPageUrl|salary|clientBrandId|companyPageUrl|companyLogoUrl|positionId|companyName|employmentType|isHighlighted|score|easyApply|employerType|workFromHomeAvailability|isRemote|debug',
                            'culture': 'en',
                            'recommendations': 'true',
                            'interactionId': '1',
                            'fj': 'true',
                            'includeRemote': 'true',
                        }
            headers = {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                        'x-api-key': '1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8',
                    }
            url = 'https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search?' + urlencode(params)
            yield scrapy.Request(url, headers=headers, meta=response.meta, callback=self.parse)


    def parse_details(self, response, item):
        details = response.xpath('//div[@id="jobDescription"]//text()').getall()
        details = ' '.join(txt.strip() for txt in details if txt.strip())
        item['JobDetails'] = details
        
        # spellchecked_description, education, job_type, industry_type, years_of_experience = self.nlp.process_job_description(details)
        # item['JobDetails'] = spellchecked_description
        # item["Industry"] = industry_type
        # item["Experience"] = years_of_experience
        # item['JobType'] = item['JobType'] if item.get("JobType") else job_type
        # item["Education"] = education
        
        yield item