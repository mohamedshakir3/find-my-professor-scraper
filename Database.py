import os
from supabase import create_client
import cohere
from openai import OpenAI
import json
import time
import pandas as pd
import requests

class Database:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
    COHERE_API_KEY = os.getenv("COHERE_API_KEY")
    
    def __init__(self, model = "embed-english-v3.0"):
        self.supabase = create_client(self.SUPABASE_URL, self.SUPABASE_ANON_KEY)
        self.cohere_client = cohere.Client(self.COHERE_API_KEY)
        self.model_name = model
        self.openai_client = OpenAI()
    
    def batch_insert(self, table, data, batch_size=250):
        """Insert data into table in smaller batches."""
        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i + batch_size]
            try:
                print(f"Inserting batch {i // batch_size + 1} of {((total - 1) // batch_size) + 1}")
                self.supabase.table(table).insert(batch).execute()
            except Exception as e:
                print(f"Failed to insert batch {i // batch_size + 1}: {e}")

    def insert(self, table, record):
        try:
            print(f"Inserting {record} into {table}")
            self.supabase.table(table).insert(record).execute()
        except Exception as e:
            print(f"Failed to insert {record} {e}")

    def get_univerities(self):
        try:
            universities = self.supabase.table("universities").execute().data
            return universities
        except Exception as e:
            print(f"Failed to fetch universities {e}")
            return []
    
    def generate_embeddings(self,text):
        try:
            response = self.cohere_client.embed(
                texts=[text],
                model=self.model_name,
                input_type="search_document"
            )

            if response and response.embeddings:
                return response.embeddings[0]

            print("No embedding returned for:", text)
            return []

        except Exception as e:
            print(f"Error generating embedding for '{text}': {e}")
            return []

    def update_professors(self, professors):
        data = []
        for i, prof in enumerate(professors):
            print(f"Processing prof: {i}, {prof}")
            name = prof.get("name", "")
            university = prof.get("university", "")
            faculty = prof.get("faculty", "")
            department = prof.get("department", "")
            website = prof.get("website", "")
            email = prof.get("email", "")
            research_interests = prof.get("research_interests", [])
            data.append({
                "name": name,
                "university": university,
                "faculty": faculty,
                "department": department,
                "website": website,
                "email": email,
                "research_interests": research_interests})

        self.batch_insert("professors", data)

    def update_research_interests(self, university=""):
        records = []
        if university:
            professors = self.supabase.table("professors").select("*").eq("university", university).execute().data
        else:
            professors = self.supabase.table("professors").select("*").execute().data
            
        count = 0
        for prof in professors:
            print(f"Processing {prof['name']}")
            research_interests = prof.get("research_interests", [])
            if not research_interests:
                continue
            research_interests = "; ".join(research_interests)
            embedding = self.generate_embeddings(research_interests)
            self.supabase.table("professors")\
                .update({"embedding": embedding})\
                .eq("id", prof["id"])\
                .execute()
            count += 1
            if count % 50 == 0:
                time.sleep(2)
            


            # for research_interest in prof.get("research_interests", []):
            #     print(f"Processing {research_interest}")
            #     embedding = self.generate_embeddings(research_interest)
            #     records.append({
            #         "research_interest": research_interest,
            #         "embedding": embedding, "prof_id": prof["id"]})

        self.batch_insert("research_interests", records)
    
    def generate_embeddings_openai(self):
        professors = self.supabase.table("professors").select("*").execute().data
        interests = []
        for prof in professors:
            for ri in prof.get("research_interests", []):
                interests.append((prof["id"], ri))
        with open("embeddings_batch.jsonl", "w", encoding="utf-8") as f:
            for i, text in enumerate(interests):
                profId, interest = text
                line = {
                    "custom_id": f"prof-{profId}-embed-{i}",
                    "method":    "POST",
                    "url":       "/v1/embeddings",
                    "body": {
                        "model": "text-embedding-3-small",
                        "input": interest
                    }
                }
                f.write(json.dumps(line) + "\n")
        
        batch_input = self.openai_client.files.create(
            file=open("embeddings_batch.jsonl", "rb"),
            purpose="batch"
        )
        batch = self.openai_client.batches.create(
            input_file_id=batch_input.id,
            endpoint="/v1/embeddings",
            completion_window="24h",
            metadata={"description":"prof-interest-embeddings"}
        )
        # output = self.openai_client.files.content(batch.output_file_id)
        # with open("embeddings_results.jsonl","w") as out:
        #     out.write(output.text)
    def update_research_interests_openai(self):
        with open('embeddings.jsonl') as f:
            embeddings = [json.loads(line) for line in f]
        with open('embeddings_batch.jsonl') as f:
            prompts = [json.loads(line) for line in f]
        records = []
        for embedding, prompt in zip(embeddings, prompts):
            profId = embedding["custom_id"].split("-", 2)[1]
            embed = embedding["response"]["body"]["data"][0]["embedding"]
            research_interest = prompt["body"]["input"]
            records.append({
                    "prof_id": profId,
                    "embedding": embed,
                    "research_interest": research_interest})

        self.batch_insert("embeddings", records)
    
    def qs_scraper(self):
        xls = pd.ExcelFile("data-dumps/rankings-by-subject.xlsx")
        subject_mapping = open("data-dumps/subject-mapping.json")
        subject_mapping = json.load(subject_mapping)
        # print(xls.sheet_names)
        university_rankings = []
        queries = []
        for row in subject_mapping:
            university = row["university"]
            faculty = row["faculty"]
            department = row["department"]
            subject = row["subject"]
            df = pd.read_excel(xls, sheet_name=subject,skiprows=10, usecols="A:D")
            rank = f"{df.tail(1)["2025"].to_list()[0].split("-")[-1]}+"
            for _, row in df.iterrows():
                if row["Country / Territory"] == "Canada" and row["Institution"] == university:
                    rank = row["2025"]
                    if rank.startswith("="):
                        rank = rank[1:]
                    break
            if university == "Université de Montréal":
                self.supabase.table("professors")\
                    .update({"ranking": rank})\
                    .eq("university", "University of Montreal")\
                    .eq("faculty", faculty)\
                    .eq("department", department)\
                    .execute()
            university_rankings.append({
                    "university": university,
                    "faculty": faculty,
                    "department": department,
                    "ranking": rank})
        
        
        with open("data-dumps/rankings.json", "w", encoding="utf-8") as f:
            json.dump(university_rankings, f, indent=4, ensure_ascii=False)

    def update_universities(self, name, university):
        print(f"Uploading {name}")
        self.supabase.table("universities").insert({"name": name, "university": university}).execute()

    def update_google_scholar(self):
        universities = {
            # "University of Ottawa": "uottawa_authors.json", 
            # "University of Waterloo": "uwaterloo_authors.json",
            # "Western University": "western_authors.json",
            # "Carleton University": "carletonu_authors.json",
            # "McGill University": "mcgill_authors.json",
            # "McMaster University": "mcmaster_authors.json",
            "University of Toronto": "uoft_authors.json",
            # "University of British Columbia": "ubc_authors.json",
            # "University of Montreal": "udem_authors.json",
            # "University of Alberta": "ualberta_authors.json",
            # "University of Calgary": "ucalgary_authors.json",
            # "University of Queens": "queens_authors.json",
            # "Concordia University": "concordia_authors.json"
        }
        
        for uni in universities:
            print(f"Updating {uni}")
            profs = self.supabase.table("professors").select("id, name, university").eq("university", uni).execute().data
            authors_json = open(f"data-dumps/{universities[uni]}")
            authors = json.load(authors_json)
            updated = 0
            print(len(authors))
            for prof in profs:
                prof_scholar = next((author for author in authors if author.get("name").lower() == prof["name"].lower()), None)
                if prof_scholar:
                    gs = prof_scholar["profile_url"]
                    cited_by = prof_scholar["cited_by"]
                    self.supabase.table("professors").update({"google_scholar": gs, "cited_by": cited_by}).eq("id", prof["id"]).execute()
                    updated += 1
            print(f"Updated {updated}/{len(profs)} profs from {uni} with their GS profile.")
            