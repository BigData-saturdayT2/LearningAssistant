import requests
from bs4 import BeautifulSoup
import snowflake.connector
import os
import logging
from pinecone import Pinecone, Index, ServerlessSpec
from nltk.tokenize import sent_tokenize
import nltk
from dotenv import load_dotenv
import re
import openai
import time
import random

# Configure logging
logging.basicConfig(filename='scraping_pinecone_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Download and specify the nltk data directory
nltk.download('punkt')
nltk.data.path.append('/Users/nishitamatlani/nltk_data')  # Update with your NLTK data path

# Load environment variables
load_dotenv()

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    'user': 'vicksInhaler',
    'password': 'vicksInhaler@123',
    'account': 'vt67315.us-east-2.aws',
    'warehouse': 'COMPUTE_WH',
    'database': 'final_project',
    'schema': 'PUBLIC'
}

# Pinecone and OpenAI configuration
PINECONE_API_KEY="pcsk_3rAxEB_5gz1Pwa7Sf9XeCXZ1L1mp3XTT4Ho83Law4JAx5M6mPcK7rRoeXGNfAaTJC4mYJt"
PINECONE_ENVIRONMENT = "us-east-1"
TEXT_INDEX_NAME = "gfg-index"
DIMENSION = 1536
METRIC = "cosine"
openai.api_key = os.getenv("OPENAI_API_KEY")

# Verify environment variables
if not PINECONE_API_KEY or not PINECONE_ENVIRONMENT or not openai.api_key:
    logging.error("API keys missing in environment variables.")
    exit(1)

# Initialize Pinecone
pc = Pinecone(api_key=PINECONE_API_KEY, environment=PINECONE_ENVIRONMENT)
if TEXT_INDEX_NAME not in pc.list_indexes().names():
    pc.create_index(
        name=TEXT_INDEX_NAME,
        dimension=DIMENSION,
        metric=METRIC,
        spec=ServerlessSpec(cloud='aws', region='us-east-1')
    )
    logging.info(f"Index '{TEXT_INDEX_NAME}' created successfully.")
else:
    logging.info(f"Index '{TEXT_INDEX_NAME}' already exists.")

text_index = pc.Index(TEXT_INDEX_NAME)

# Download NLTK tokenizer
nltk.download("punkt")


# Function to fetch links from Snowflake
def fetch_links_from_snowflake():
    try:
        logging.info("Connecting to Snowflake to fetch links.")
        connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = connection.cursor()
        cursor.execute('SELECT ID, LINK, TITLE FROM TECH_LINKS WHERE STATUS = \'NEW\'')
        links = cursor.fetchall()
        cursor.close()
        connection.close()
        logging.info(f"Fetched {len(links)} links from Snowflake.")
        return links
    except snowflake.connector.Error as e:
        logging.error(f"Error while fetching links from Snowflake: {e}")
        return []
    

def mark_links_as_processed(link_ids):
    """
    Update the STATUS of processed links in Snowflake to 'PROCESSED'.
    """
    try:
        # Input validation
        if not isinstance(link_ids, list) or not all(isinstance(link_id, int) for link_id in link_ids):
            logging.error("Invalid link_ids provided. Expected a list of integers.")
            raise ValueError("link_ids must be a list of integers.")

        # Connect to Snowflake
        connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = connection.cursor()
        
        # Debug log for IDs being processed
        logging.debug(f"Processing link IDs: {link_ids}")
        
        # Update query
        update_query = "UPDATE TECH_LINKS SET STATUS = 'PROCESSED' WHERE ID = %s"
        cursor.executemany(update_query, [(link_id,) for link_id in link_ids])
        
        # Commit changes
        try:
            connection.commit()
            logging.info(f"Successfully updated {len(link_ids)} links to PROCESSED.")
        except Exception as commit_error:
            logging.error(f"Error committing changes: {commit_error}")
            raise

        # Close resources
        cursor.close()
        connection.close()

    except Exception as e:
        logging.error(f"Error updating link status in Snowflake: {e}")
        raise



# Function to scrape webpage content
def scrape_webpage(page_url):
    try:
        logging.info(f"Scraping page: {page_url}")
        resp = requests.get(page_url, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            article_title = soup.find('div', class_='article-title')
            article_title = article_title.get_text(strip=True) if article_title else "Untitled"
            main_div = soup.find('div', class_='text')
            if main_div:
                paragraphs = [p.get_text(strip=True) for p in main_div.find_all('p')]
                return article_title, " ".join(paragraphs)
            else:
                logging.warning(f"No main content found for {page_url}")
                return article_title, ""
        else:
            logging.error(f"Failed to fetch page {page_url}. Status code: {resp.status_code}")
            return None, None
    except Exception as e:
        logging.error(f"Error scraping page {page_url}: {e}")
        return None, None


# Function to clean text
def clean_text(text):
    text = text.replace('\n', ' ')
    text = re.sub(r'[^a-zA-Z0-9\s.,]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# Function to chunk text
def chunk_text(text, max_chars=500, overlap_sentences=1):
    sentences = sent_tokenize(text)
    chunks = []
    current_chunk = []
    current_chunk_char_count = 0
    i = 0
    while i < len(sentences):
        sentence = sentences[i]
        if current_chunk_char_count + len(sentence) > max_chars:
            chunks.append(" ".join(current_chunk).strip())
            current_chunk = current_chunk[-overlap_sentences:]
            current_chunk_char_count = sum(len(s) for s in current_chunk)
        current_chunk.append(sentence)
        current_chunk_char_count += len(sentence)
        i += 1
    if current_chunk:
        chunks.append(" ".join(current_chunk).strip())
    return chunks


# Function to generate embeddings
def get_ada_embedding(text):
    try:
        response = openai.Embedding.create(
            model="text-embedding-ada-002",
            input=text
        )
        return response["data"][0]["embedding"]
    except Exception as e:
        logging.error(f"Error generating ADA embedding for text: {e}")
        return None


# Function to upload embeddings to Pinecone
def upload_to_pinecone(embeddings):
    try:
        text_index.upsert(vectors=embeddings)
        logging.info("Uploaded embeddings to Pinecone.")
    except Exception as e:
        logging.error(f"Error uploading embeddings to Pinecone: {e}")


# Main processing function
def process_links():
    links = fetch_links_from_snowflake()
    for article_id, page_url, title in links:
        article_title, text = scrape_webpage(page_url)
        if text:
            cleaned_text = clean_text(text)
            chunks = chunk_text(cleaned_text)
            embeddings = []
            for idx, chunk in enumerate(chunks):
                embedding = get_ada_embedding(chunk)
                if embedding:
                    embeddings.append({
                        "id": f"{article_id}-{idx}",
                        "values": embedding,
                        "metadata": {"article_id": article_id, "chunk_id": idx, "title": article_title, "text": chunk}
                    })
            if embeddings:
                upload_to_pinecone(embeddings)


# Main execution
if __name__ == "__main__":
    logging.info("Starting the scraping and Pinecone upload process.")
    process_links()
    logging.info("Process completed.")