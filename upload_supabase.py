import json
import os
from supabase import create_client
import cohere

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")


supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
# co = cohere.Client(COHERE_API_KEY)


model_name = "embed-english-v3.0"
# tokenizer = AutoTokenizer.from_pretrained(model_name)
# model = AutoModel.from_pretrained(model_name)
def update_profs(profs): 
    data = []
    research_data = []
    for i, prof in enumerate(profs):
        print(f"Processing prof: {i}, {prof}")
        name = prof.get("name", "")
        prev_universities = prof.get("previous_education", {})
        university = prof.get("current_university", "")
        email = prof.get("email", "")
        website = prof.get("website_link", "")
        research_interests = prof.get("research_interests", [])
        faculty = prof.get("faculty", "")
        department = prof.get("department", "")
        if type(research_interests) is str: 
            research_interests = research_interests.split(",")     
        if not university: 
            continue
        data.append(
            {
                "id": i,
                "name": name,
                "university": university,
                "faculty": faculty, 
                "department": department, 
                "email": email,
                "website": website, 
                "research_interests": research_interests
            }
        )
        if research_interests:
            for ri in research_interests:
                text_to_embed = ri.replace(" ", "_").lower() 
                embedding = get_embedding(text_to_embed)
                research_data.append(
                    {"research_interest": ri, "prof_id": i, "embedding": embedding})
            
            
    with open("professors_table.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    with open("research_interets.json", "w", encoding="utf-8") as f:
        json.dump(research_data, f, indent=4, ensure_ascii=False)
    batch_insert("professors", data)
    batch_insert("research_interests", research_data)

def batch_insert(table, data, batch_size=250):
    """Insert data into table in smaller batches."""
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        try:
            print(f"Inserting batch {i // batch_size + 1} of {((total - 1) // batch_size) + 1}")
            supabase.table(table).insert(batch).execute()
        except Exception as e:
            print(f"Failed to insert batch {i // batch_size + 1}: {e}")

def update_reference_data():
    profs = supabase.table("professors").select("*").execute().data
    departments, faculties, universities, research_interests = set(), set(), set(), set()
    data = []
    for prof in profs:
        print(f"Processing prof {prof['id']} {prof['name']}")
        research_interest = prof.get("research_interests", [])
        if type(research_interest) is str: 
            research_interest = research_interest.split(",")
        if not research_interest: 
            research_interest = []
        for ri in research_interest: 
            if ri not in research_interests:
                print(f"Research interest: {ri}")
                clean_string = ri.replace(" ", "_").lower()
                embedding = get_embedding(clean_string)
                data.append({"research_interest": ri, "prof_id": prof["id"],
                             "embedding": embedding})
                research_interests.add(ri)
    supabase.table("research_interests").insert(data).execute()
        # if university not in universities: 
        #     supabase.table("universities").insert({
        #         "university": university
        #     }).execute()
        #     universities.add(university)
        # if faculty not in faculties:
        #     supabase.table("faculties").insert({
        #         "faculty": faculty
        #     }).execute()
        #     faculties.add(faculty)
        # if department not in departments: 
        #     supabase.table("departments").insert({
        #         "department": department
        #     }).execute()
        #     departments.add(department)
        
def get_embedding(text):
    try:
        response = co.embed(
            texts=[text],
            model="embed-english-v3.0",
            input_type="search_document"  # Use "search_query" for user queries
        )

        if response and response.embeddings:
            return response.embeddings[0]  # Return the 1536-dimension embedding

        print("No embedding returned for:", text)
        return []

    except Exception as e:
        print(f"Error generating embedding for '{text}': {e}")
        return []

# def get_embedding(text):
#     inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
#     with torch.no_grad():
#         outputs = model(**inputs)
#     return outputs.last_hidden_state.mean(dim=1).squeeze().tolist()

def get_embedding(text):
    try:
        response = co.embed(
            texts=[text],
            model="embed-english-v3.0",
            input_type="search_document"  # Use "search_query" for user queries
        )

        if response and response.embeddings:
            return response.embeddings[0]  # Return the 1536-dimension embedding

        print("No embedding returned for:", text)
        return []

    except Exception as e:
        print(f"Error generating embedding for '{text}': {e}")
        return []
    
def generate_embeddings():
    research_interests = supabase.table("research_interests").select("*").execute().data
    count = 0
    for ri in research_interests:
        count += 1
      
        text_to_embed = ri.get("research_interests", [])
        ri_id = ri["id"]
        
        embedding = get_embedding(text_to_embed)

        supabase.table("research_interests").update({"embedding": embedding}).eq("id", ri_id).execute()
        if count == 10: 
            break


def update_universities(name, university):
    print(f"Uploading {name}")
    supabase.table("universities").insert({"name": name, "university": university}).execute()
    # supabase.table("universities").update({"university": university}).eq("name", name)


if __name__ == "__main__":
    file = open("universities.json")
    universities = json.load(file)
    for uni in universities:
        update_universities(uni, universities[uni])