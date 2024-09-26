import aiohttp
import asyncio
from urllib.parse import urlencode
import json
import datetime
from uuid import uuid4
from bs4 import BeautifulSoup
import logging

# Utility functions (You should replace them with your actual implementation)
from Dice.utils import NLPUtils, validate, find_payout_term, foramt_salary

def hash_dict(item):
    dict_str = json.dumps(item, sort_keys=True)
    return hash(dict_str)

today = datetime.datetime.today().strftime('%Y-%m-%d')

class JobpostsSpider:
    def __init__(self, logger):
        self.logger = logger or logging.getLogger(__name__)
        self.nlp = NLPUtils(self.logger)
        self.unique_items = set()
        self.all_jobs = []  # New list to store all job data

    async def fetch(self, session, url, headers, params=None):
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                self.logger.info(f"Successfully fetched data from {url}")
                return await response.json()
            self.logger.error(f"Failed to fetch data from {url}. Status code: {response.status}")
            return None

    async def fetch_html(self, session, url, headers):
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                self.logger.info(f"Successfully fetched HTML from {url}")
                return await response.text()
            self.logger.error(f"Failed to fetch HTML from {url}. Status code: {response.status}")
            return None

    async def parse_details(self, session, item, headers):
        html_content = await self.fetch_html(session, item['PostUrl'], headers)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            details = soup.select_one('#jobDescription').get_text(separator=' ').strip()
            item['JobDetails'] = details
            self.logger.info(f"Successfully parsed details for job {item['JobID']}")
        else:
            self.logger.warning(f"Failed to parse details for job {item['JobID']}")
        return item

    async def parse(self, session, data, state, headers):
        tasks = []
        for record in data['data']:
            print(record)
            item = {}
            title = self.nlp.extract_job_title(validate(record, ['title']))
            item['Domain'] = 'www.dice.com'
            item['PostUrl'] = record['detailsPageUrl']
            item['JobID'] = record['id']
            title = title if title else validate(record, ['title'])
            item['Title'] = self.nlp.spellcheck(title)
            location = record['jobLocation']['displayName'].split(',')
            item['City'] = location[0].strip()
            item['State'] = location[1].strip()
            item['Speciality'] = ''
            item['JobType'] = validate(record, ['employmentType'])
            item['Company'] = validate(record, ['companyName'])
            item['PostedOn'] = record['postedDate'].split('T')[0]
            item['SalaryFrom'] = item['SalaryUpto'] = item['PayoutTerm'] = ''

            salary = validate(record, ['salary'])
            if not salary:
                self.logger.warning(f"No salary information for job {item['JobID']}")
                continue
            
            item["SalaryInfo"] = validate(record, ['salary'])
            item['SalaryTotal'] = salary
            if salary:
                payout = find_payout_term(salary)
                salary_parts = salary.split('-')
                item['SalaryFrom'], pay_out = foramt_salary(salary_parts[0], payout)
                item['SalaryUpto'], pay_out = foramt_salary(salary_parts[-1], payout)
                if item['SalaryFrom'] <= 0:
                    item['SalaryFrom'] = item['SalaryUpto']
                item['PayoutTerm'] = pay_out

            item['IsEstimatedSalary'] = ''
            item['ScrapedOn'] = today

            task = asyncio.create_task(self.parse_details(session, item, headers))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        self.logger.info(f"Parsed {len(results)} jobs for state {state[0]}")
        self.all_jobs.extend(results)  # Add parsed jobs to the all_jobs list
        return results

    async def start_requests(self):
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
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'x-api-key': '1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8',
        }

        async with aiohttp.ClientSession() as session:
            for state in locations:
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
                    'fields': 'id|jobId|guid|summary|title|postedDate|modifiedDate|jobLocation.displayName|detailsPageUrl|salary|clientBrandId|companyPageUrl|companyLogoUrl|positionId|companyName|employmentType|isHighlighted|score|easyApply|employerType|workFromHomeAvailability|isRemote|debug',
                    'culture': 'en',
                    'recommendations': 'true',
                    'interactionId': '1',
                    'fj': 'true',
                    'includeRemote': 'true',
                }
                url = f'https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search?{urlencode(params)}'
                data = await self.fetch(session, url, headers)

                if data and 'data' in data:
                    await self.parse(session, data, state, headers)
                else:
                    self.logger.error(f"No data received for state {state[0]}")
                
                # Handle pagination (simplified for the first page only)
                if data and data['meta']['currentPage'] < data['meta']['pageCount']:
                    current_page = data['meta']['currentPage'] + 1
                    params['page'] = str(current_page)
                    url = f'https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search?{urlencode(params)}'
                    data = await self.fetch(session, url, headers)
                    if data and 'data' in data:
                        await self.parse(session, data, state, headers)
                    else:
                        self.logger.error(f"No data received for state {state[0]} on page {current_page}")

        # Save all jobs to a JSON file
        with open('aio_job.json', 'w') as f:
            json.dump(self.all_jobs, f, indent=2)
        self.logger.info(f"Saved {len(self.all_jobs)} jobs to aio_job.json")


# Run the spider
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('JobpostsSpider')
    spider = JobpostsSpider(logger=logger)
    asyncio.run(spider.start_requests())
