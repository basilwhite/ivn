"""
Script: generate_ivn_recommendations_rag.py

Purpose:
Generate rich, strategic recommendations for WS NLI using GPT-4 and Retrieval-Augmented Generation (RAG).
Retrieves relevant best practices, previous recommendations, or domain-specific guidance from a document store
and injects them into the prompt, making recommendations more actionable and grounded in organizational knowledge.

Requirements:
- openai>=1.0.0
- pandas
- chromadb
- sentence-transformers
"""

import pandas as pd
import openai
import time
import chromadb
from chromadb.utils import embedding_functions
from sentence_transformers import SentenceTransformer

# Set your OpenAI API key securely
client = openai.OpenAI(api_key="sk-...")  # <-- Insert your API key here

# Settings
INPUT_FILE = "ivntest.xlsx"
OUTPUT_FILE = "generated_recommendations.xlsx"
SAVE_INTERVAL = 5  # Save every N rows

# RAG Settings
CHROMA_DB_DIR = "rag_db"
CHROMA_COLLECTION = "ivn_knowledge"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
TOP_K = 3  # Number of relevant docs to retrieve

# Initialize embedding model
embedder = SentenceTransformer(EMBEDDING_MODEL)

# Initialize ChromaDB client and collection
chroma_client = chromadb.PersistentClient(path=CHROMA_DB_DIR)
if CHROMA_COLLECTION not in [c.name for c in chroma_client.list_collections()]:
    # If the collection doesn't exist, create it (empty)
    chroma_client.create_collection(name=CHROMA_COLLECTION)
collection = chroma_client.get_collection(
    name=CHROMA_COLLECTION,
    embedding_function=embedding_functions.SentenceTransformerEmbeddingFunction(EMBEDDING_MODEL)
)

def ensure_knowledge_base_populated():
    """
    Populate the ChromaDB collection with IVN best practices, previous recommendations, or domain-specific guidance
    if not already present. This is a one-time setup step.
    """
    if collection.count() > 0:
        return  # Already populated

    # Example: Load knowledge base from a CSV or Excel file
    # For demonstration, we use a hardcoded list. Replace with your actual data source.
    knowledge_entries = [
        {
            "id": "1",
            "text": "Engage local stakeholders early to ensure buy-in and adapt solutions to regional contexts.",
            "source": "IVN Best Practices 2023"
        },
        {
            "id": "2",
            "text": "Leverage cross-agency partnerships to maximize resource sharing and knowledge transfer.",
            "source": "USDA WS Guidance"
        },
        {
            "id": "3",
            "text": "Document and share lessons learned from pilot projects to inform future initiatives.",
            "source": "NLI Recommendations Archive"
        },
        # Add more entries as needed
    ]
    ids = [entry["id"] for entry in knowledge_entries]
    texts = [entry["text"] for entry in knowledge_entries]
    metadatas = [{"source": entry["source"]} for entry in knowledge_entries]
    collection.add(documents=texts, metadatas=metadatas, ids=ids)

def retrieve_relevant_knowledge(enabling_desc, dependent_desc, top_k=TOP_K):
    """
    Retrieve top-k relevant knowledge base entries for the given context.
    """
    query = f"{enabling_desc} {dependent_desc}"
    results = collection.query(
        query_texts=[query],
        n_results=top_k
    )
    retrieved = []
    for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
        retrieved.append(f"- {doc} (Source: {meta.get('source', 'Unknown')})")
    return "\n".join(retrieved) if retrieved else "No relevant best practices found."

def generate_recommendation(enabling_desc, dependent_desc):
    # Retrieve relevant knowledge from the RAG pipeline
    relevant_knowledge = retrieve_relevant_knowledge(enabling_desc, dependent_desc)

    prompt = f"""
You are a policy analyst generating rich, strategic recommendations for the USDA Wildlife Services Nonlethal Initiative (WS NLI).
Given the following context:

Enabling Component Description:
"{enabling_desc}"

Dependent Component Description:
"{dependent_desc}"

Relevant Best Practices, Previous Recommendations, or Domain Guidance:
{relevant_knowledge}

Using the above, generate a unique, actionable, and insightful recommendation explaining how the Enabling Component can progress the Dependent Component.
Ground your recommendation in the retrieved best practices or guidance, making it as specific and explainable as possible.
Focus on strategic clarity, stakeholder value, and alignment with broader WS NLI goals.
Avoid generic or vague language.
If you use a best practice or guidance, cite its source in parentheses.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content.strip()

    except openai.RateLimitError:
        print("Rate limit hit. Waiting 60 seconds...")
        time.sleep(60)
        return generate_recommendation(enabling_desc, dependent_desc)

    except openai.APIError as e:
        print(f"API error: {e}. Retrying in 30 seconds...")
        time.sleep(30)
        return generate_recommendation(enabling_desc, dependent_desc)

    except Exception as e:
        print(f"Unexpected error: {e}")
        return "ERROR: " + str(e)

def main():
    ensure_knowledge_base_populated()

    try:
        df = pd.read_excel(OUTPUT_FILE)  # Try to resume from output file
        print(f"Resuming from {OUTPUT_FILE}")
    except FileNotFoundError:
        df = pd.read_excel(INPUT_FILE)
        df["Recommendation"] = ""

    for idx, row in df.iterrows():
        if pd.notna(row["Recommendation"]) and str(row["Recommendation"]).strip() != "":
            continue  # Skip completed rows

        if pd.isna(row["Enabling Component Description"]) or pd.isna(row["Dependent Component Description"]):
            continue  # Skip rows without both descriptions

        print(f"Generating recommendation for row {idx+1}...")
        rec = generate_recommendation(str(row["Enabling Component Description"]), str(row["Dependent Component Description"]))
        df.at[idx, "Recommendation"] = rec

        if idx % SAVE_INTERVAL == 0:
            df.to_excel(OUTPUT_FILE, index=False)
            print(f"Progress saved at row {idx+1}")

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"All recommendations saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
