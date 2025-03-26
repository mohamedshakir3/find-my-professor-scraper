import os
from supabase import create_client
import cohere

class Database:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
    COHERE_API_KEY = os.getenv("COHERE_API_KEY")
    
    def __init__(self, model = "embed-english-v3.0"):
        self.supabase = create_client(self.SUPABASE_URL, self.SUPABASE_ANON_KEY)
        self.cohere_client = cohere.Client(self.COHERE_API_KEY)
        self.model_name = model
    
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
            
        
        for prof in professors:
            print(f"Processing {prof["name"]}")
            for research_interest in prof.get("research_interests", []):
                print(f"Processing {research_interest}")
                embedding = self.generate_embeddings(research_interest)
                records.append({
                    "research_interest": research_interest,
                    "embedding": embedding, "prof_id": prof["id"]})

        self.batch_insert("research_interests", records)

    def update_universities(self, name, university):
        print(f"Uploading {name}")
        self.supabase.table("universities").insert({"name": name, "university": university}).execute()