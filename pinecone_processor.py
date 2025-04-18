import json
import logging
import os
import re
import pdfplumber
import torch
import io
from google.cloud import storage
from google.api_core import exceptions as gcs_exceptions
from pdfminer.pdfparser import PDFSyntaxError
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from openai import OpenAI
from pinecone import Pinecone, ServerlessSpec

# Logger setup
logger = logging.getLogger(__name__)

# Initialize clients
openai_client = OpenAI(api_key="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")
pc = Pinecone(api_key='pcsk_5kwnzz_RjyAj464745fCCuz3S3CPF5Anf2Ktk9ww3CRifWXb1rYRhUvXmUNhtFxJthvnBj')

index_name = "trace-index"
dimension = 1536

if index_name not in pc.list_indexes().names():
    pc.create_index(
        name=index_name,
        dimension=dimension,
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1")
    )
index = pc.Index(index_name)

# Sentiment model
tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")
model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")

# ---- PDF Utilities ----

def extract_qna_chunks(text):
    pattern = re.compile(r"(Q:.*?)(?=\nQ:|\Z)", re.DOTALL)
    return [chunk.strip() for chunk in pattern.findall(text) if len(chunk.strip()) > 30]

def extract_question_title(chunk):
    return chunk.splitlines()[0].replace("Q:", "").strip()

def count_responses(chunk):
    return len(re.findall(r"\n\d{1,2}[\). ]", chunk)) or 1

def get_sentiment(text, max_tokens=512):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=max_tokens)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = softmax(logits.numpy()[0])
    labels = ["negative", "neutral", "positive"]
    return dict(zip(labels, map(float, probs)))

def chunk_already_indexed(chunk_id):
    try:
        result = index.fetch(ids=[chunk_id])
        return chunk_id in result.vectors
    except Exception as e:
        logger.warning(f"⚠️ Error checking chunk ID {chunk_id}: {e}")
        return False

def embed_and_upsert_chunks(chunks, pdf_name):
    batch = []
    for i, chunk in enumerate(chunks):
        chunk_id = f"{pdf_name}_chunk_{i}"
        if chunk_already_indexed(chunk_id):
            logger.info(f"⏩ Chunk already indexed: {chunk_id}")
            continue

        question = extract_question_title(chunk)
        comment_count = count_responses(chunk)
        sentiment_scores = get_sentiment(chunk)

        embedding = openai_client.embeddings.create(
            input=[chunk],
            model="text-embedding-ada-002"
        ).data[0].embedding

        metadata = {
            "question": question,
            "source_file": pdf_name,
            "chunk_id": i,
            "comment_count": comment_count,
            "sentiment_positive": round(sentiment_scores["positive"], 4),
            "sentiment_neutral": round(sentiment_scores["neutral"], 4),
            "sentiment_negative": round(sentiment_scores["negative"], 4),
            "text": chunk
        }

        batch.append((chunk_id, embedding, metadata))

        if len(batch) >= 100:
            index.upsert(batch)
            batch = []

    if batch:
        index.upsert(batch)
    logger.info(f"✅ Upserted {len(chunks)} chunks from {pdf_name}")

# ---- Main Pipeline ----

def push_to_pinecone(message):
    try:
        decoded_message = message.decode("utf-8")
        message_data = json.loads(decoded_message)
        logger.info(f"Processing message: {decoded_message}")

        folder_path = message_data.get("folderPath")
        file_name = message_data.get("filename")
        if not folder_path or not file_name:
            raise ValueError("folderPath or filename missing in message")

        bucket_name = "your-bucket-name-001"
        folder_path = folder_path.rstrip("/")
        blob_name = f"{folder_path}/{file_name}".replace("\\", "/")
        logger.info(f"Constructed blob name: {blob_name}")

        # Download file from GCS
        storage_client = storage.Client(project="gcp-dev-7125")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        pdf_bytes = blob.download_as_bytes()
        pdf_file_obj = io.BytesIO(pdf_bytes)
        logger.info(f"📄 Fetched PDF from GCS: gs://{bucket_name}/{blob_name}")

        # Extract text from PDF
        text = ""
        with pdfplumber.open(pdf_file_obj) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text

        if not text.strip():
            raise ValueError("Empty or unprocessable PDF content")

        # Extract and process Q&A
        chunks = extract_qna_chunks(text)
        if not chunks:
            raise ValueError("No valid Q&A chunks found")

        embed_and_upsert_chunks(chunks, file_name)

        return True

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message: {e}")
        return False
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return False
    except gcs_exceptions.GoogleAPIError as e:
        logger.error(f"GCP Storage error: {e}")
        return False
    except PDFSyntaxError as e:
        logger.error(f"Invalid PDF file: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}")
        return False
