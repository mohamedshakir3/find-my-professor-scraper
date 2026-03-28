import json
from scraper.db.repositories import get_professors_ready_for_ai
from scraper.db.connection import close_pool

def export_to_jsonl(filename="cloud_input.jsonl"):
    print("Packing the briefcase...")
    # Grab all professors who have Markdown but no AI extraction yet
    rows = get_professors_ready_for_ai(limit=10000) 
    
    with open(filename, 'w') as f:
        for row in rows:
            # We only send what the LLM needs: ID, Name, and Markdown
            briefcase_item = {
                "id": row["id"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "profile_markdown": row["profile_markdown"]
            }
            f.write(json.dumps(briefcase_item) + "\n")
            
    print(f"✅ Exported {len(rows)} profiles to {filename}")
    close_pool()

if __name__ == "__main__":
    export_to_jsonl()