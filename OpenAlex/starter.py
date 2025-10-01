import io

import fitz  # PyMuPDF, easier for PDF → image
import pdfplumber
import pytesseract
import requests
from PIL import Image
from transformers import pipeline


# 1. Query OpenAlex authors affiliated w/ Stony Brook
def get_stonybrook_authors(per_page=5):
    url = "https://api.openalex.org/authors"
    params = {
        "filter": "last_known_institution.display_name.search:stonybrook",
        "per_page": per_page,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()["results"]


# 2. Get works for an author
def get_author_works(author_id, per_page=5):
    url = "https://api.openalex.org/works"
    params = {"filter": f"authorships.author.id:{author_id}", "per_page": per_page}
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()["results"]


# 3. Download PDF if available
def download_pdf(work):
    oa_url = work.get("open_access", {}).get("oa_url")
    if oa_url:
        r = requests.get(oa_url)
        if r.headers.get("Content-Type") == "application/pdf":
            return r.content
    return None


# 4a. Extract text from PDF (text-based)
def extract_text_pdf(pdf_bytes):
    text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text.strip()


# 4b. OCR fallback if mostly images
def extract_text_ocr(pdf_bytes):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    text = ""
    for page in doc:
        pix = page.get_pixmap()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        text += pytesseract.image_to_string(img)
    return text.strip()


# 5. Summarize / keyword-check w/ HuggingFace
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")


def summarize_text(text, max_chars=2000):
    if len(text) > max_chars:
        text = text[:max_chars]  # truncate for test
    return summarizer(text, max_length=100, min_length=30, do_sample=False)[0][
        "summary_text"
    ]


# --- Demo run ---
if __name__ == "__main__":
    authors = get_stonybrook_authors()
    for a in authors:
        print(f"\nAuthor: {a['display_name']}")
        works = get_author_works(a["id"])
        for w in works:
            print("  Work:", w["title"])
            pdf_bytes = download_pdf(w)
            if pdf_bytes:
                text = extract_text_pdf(pdf_bytes)
                if len(text) < 50:  # maybe image-based
                    text = extract_text_ocr(pdf_bytes)
                summary = summarize_text(text)
                print("   -> Summary:", summary[:200], "...")
