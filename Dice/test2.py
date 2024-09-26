import json
import asyncio
import aiohttp

prompt = '''Extract the required years of experience for different skills, education, and industry type from the job description.
Respond with a JSON object containing the following keys: 
"required_years_of_experience" (with skills as subkeys and their respective experience in years), "education", and "industry_type". 
If any field is missing or not mentioned in the job description, return "Not Mentioned" for that field. 
Only include the relevant data in the JSON format without additional explanations or comments.

Sample output:
{
"required_years_of_experience": {
    "java": 10,
    "javacloud": 10,
    "webservices": 5,
    "micro_services": 3,
    "akaintegration": 3,
    "springcloud": "Not Mentioned",
    "cloudtechnologies": "Not Mentioned",
    "agile": "Not Mentioned"
},
"education": "BA/BS",
"industry_type": "Financial Services"
}
'''

async def get_ai_response(user_message):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer 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"
                },
                json={
                    "model": "openchat/openchat-7b:free",
                    "messages": [
                        {"role": "user", "content": user_message + "\n\n" + prompt}
                    ],
                    "temperature":0.8,
                    "repetition_penalty": 1
                }
            ) as response:
                response_json = await response.json()

                # Try to extract and parse the JSON result
                try:
                    content = response_json["choices"][0]["message"]["content"]
                    # Ensure the response is valid JSON, strip unwanted formatting if necessary
                    return json.loads(content.strip("```"))
                except (json.JSONDecodeError, KeyError):
                    # Return a default format in case of an error
                    return {
                        "required_years_of_experience": "Not Mentioned",
                        "education": "Not Mentioned",
                        "industry_type": "Not Mentioned"
                    }
    except Exception as e:
        # Catch any network or general errors and return a default format
        return {
            "required_years_of_experience": "Not Mentioned",
            "education": "Not Mentioned",
            "industry_type": "Not Mentioned",
            "error": str(e)
        }

async def process_job_descriptions(jd_list):
    tasks = [get_ai_response(jd) for jd in jd_list]
    return await asyncio.gather(*tasks)

JOB_DESCRIPTIONS = [
    "Job DescriptionJob DescriptionMinimum 1+ years school psychologist experience required. VocoVision is seeking a School Psychologist for a the 2024-2025 school year in Vermont. VocoVision utilizes a complete online platform to allow clinicians to provide services to students across the U.S. A school district in Vermont has partnered with VocoVision to find a School Psychologist able to provide their services remotely for the 2024-2025 school year. Job Type: Full time: 37.5 hours/weekK-121099 Cont",
    "Outlier helps the world's most innovative companies improve their AI models by providing human feedback. Are you an experienced  Math Expertwho would like to lend your expertise to train AI models?  About the opportunity: Outlier is looking for talented Math Experts to help train generative artificial intelligence modelsThis freelance opportunity is remote and hours are flexible, so you can work whenever is best for you You may contribute your expertise by  Assessing the factuality and relevance"
]

async def main():
    results = await process_job_descriptions(JOB_DESCRIPTIONS)
    for i, result in enumerate(results, 1):
        print(f"Job Description {i}:")
        print(json.dumps(result, indent=2))
        print()

if __name__ == "__main__":
    asyncio.run(main())
