from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Import functions from external scripts
from extraction import (
    fetch_links_from_snowflake,
    scrape_webpage,
    clean_text,
    chunk_text,
    get_ada_embedding,
    upload_to_pinecone,
    mark_links_as_processed
)
from links import scrape_tech_links, insert_into_snowflake_bulk, BASE_URL, TECH_KEYWORDS

# Configure logging to print directly to the Airflow logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='scraping_dag',
    default_args=default_args,
    description='DAG to scrape, process, and upload embeddings to Pinecone',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Scrape links from GeeksforGeeks
    def scrape_links_task():
        logging.info("Scraping links from GeeksforGeeks...")
        tech_links = scrape_tech_links(BASE_URL, TECH_KEYWORDS)
        if tech_links:
            logging.info(f"Scraped {len(tech_links)} links.")
            insert_into_snowflake_bulk(tech_links)
        else:
            logging.info("No links were scraped.")

    # Task 2: Fetch new links from Snowflake
    def fetch_new_links_task(ti):
        logging.info("Fetching new links from Snowflake...")
        links = fetch_links_from_snowflake()  # Fetch only links with STATUS='NEW'
        if links:
            logging.info(f"Fetched {len(links)} new links from Snowflake.")
            ti.xcom_push(key='links', value=links)
        else:
            logging.info("No new links to process.")

    # Task 3: Process new links
    def process_new_links_task(ti):
        logging.info("Processing links...")
        links = ti.xcom_pull(key='links', task_ids='fetch_new_links')
        if not links:
            logging.info("No links to process.")
            return

        processed_ids = []

        for article_id, page_url, title in links:
            try:
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
                                "metadata": {
                                    "article_id": article_id,
                                    "chunk_id": idx,
                                    "title": article_title,
                                    "text": chunk
                                }
                            })
                    if embeddings:
                        upload_to_pinecone(embeddings)
                        processed_ids.append(article_id)  # Add the ID to the processed list
            except Exception as e:
                logging.error(f"Error processing link {page_url}: {e}")

        # Mark processed links in Snowflake
        if processed_ids:
            logging.info(f"Marking processed IDs: {processed_ids}")
            mark_links_as_processed(processed_ids)

    # Define tasks
    scrape_links = PythonOperator(
        task_id='scrape_links',
        python_callable=scrape_links_task,
    )

    fetch_new_links = PythonOperator(
        task_id='fetch_new_links',
        python_callable=fetch_new_links_task,
    )

    process_new_links = PythonOperator(
        task_id='process_new_links',
        python_callable=process_new_links_task,
    )

    # Task dependencies
    scrape_links >> fetch_new_links >> process_new_links
