"""
OpenAlex API Pipeline for Stony Brook University Research
Queries authors, retrieves publications, processes PDFs, and validates content.
"""

import os
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

# PDF processing libraries
try:
    import pdfplumber
    import PyPDF2
except ImportError:
    print("Install PDF libraries: pip install PyPDF2 pdfplumber")

# OCR libraries (optional)
try:
    import pytesseract
    from pdf2image import convert_from_path

    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("OCR not available. Install: pip install pdf2image pytesseract pillow")

# Language model libraries (optional)
try:
    from transformers import pipeline

    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    print("Transformers not available. Install: pip install transformers torch")


@dataclass
class Author:
    """Represents an OpenAlex author"""

    id: str
    name: str
    works_count: int
    cited_by_count: int
    affiliations: List[str]


@dataclass
class Publication:
    """Represents an OpenAlex work/publication"""

    id: str
    title: str
    doi: Optional[str]
    publication_year: int
    pdf_url: Optional[str]
    authors: List[str]
    abstract: Optional[str]


class OpenAlexAPI:
    """Handles OpenAlex API interactions"""

    BASE_URL = "https://api.openalex.org"

    def __init__(self, email: Optional[str] = None):
        """
        Initialize API client.

        Args:
            email: Your email for polite pool (faster, recommended)
        """
        self.email = email
        self.session = requests.Session()
        if email:
            self.session.params = {"mailto": email}

    def find_stonybrook_authors(self, max_results: int = 25) -> List[Author]:
        """
        Find authors affiliated with Stony Brook University.

        Uses institution ROR ID for accurate matching.
        Stony Brook University ROR: https://ror.org/05qghxh33
        Stony Brook Medicine: https://ror.org/05wyq9e07
        State University of New York: https://ror.org/01q1z8k08
        """
        authors = []

        # Stony Brook University ROR ID (just the identifier, not full URL)
        stonybrook_ror = "05qghxh33"

        # Query for authors with Stony Brook affiliation
        url = f"{self.BASE_URL}/authors"
        params = {
            "filter": f"last_known_institutions.ror:{stonybrook_ror}",
            "per-page": min(max_results, 200),
            "sort": "cited_by_count:desc",  # Get most cited authors first
        }

        print("Querying OpenAlex for Stony Brook authors...")
        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        for result in data.get("results", [])[:max_results]:
            affiliations = [
                inst.get("display_name", "")
                for inst in result.get("last_known_institutions", [])
            ]

            author = Author(
                id=result["id"],
                name=result.get("display_name", "Unknown"),
                works_count=result.get("works_count", 0),
                cited_by_count=result.get("cited_by_count", 0),
                affiliations=affiliations,
            )
            authors.append(author)
            print(f"  Found: {author.name} ({author.works_count} works)")

        return authors

    def get_author_publications(
        self, author_id: str, max_results: int = 10
    ) -> List[Publication]:
        """
        Get publications for a specific author.

        Args:
            author_id: OpenAlex author ID
            max_results: Maximum number of publications to retrieve
        """
        publications = []

        url = f"{self.BASE_URL}/works"
        params = {
            "filter": f"authorships.author.id:{author_id}",
            "per-page": min(max_results, 200),
            "sort": "publication_year:desc",
        }

        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        for result in data.get("results", [])[:max_results]:
            # Extract PDF URL if available
            pdf_url = None
            if result.get("open_access", {}).get("is_oa"):
                pdf_url = result.get("open_access", {}).get("oa_url")

            # Extract author names
            author_names = [
                authorship.get("author", {}).get("display_name", "Unknown")
                for authorship in result.get("authorships", [])
            ]

            pub = Publication(
                id=result["id"],
                title=result.get("title", "Untitled"),
                doi=result.get("doi"),
                publication_year=result.get("publication_year", 0),
                pdf_url=pdf_url,
                authors=author_names[:5],  # Limit to first 5 authors
                abstract=result.get("abstract"),
            )
            publications.append(pub)

        return publications


class PDFProcessor:
    """Handles PDF downloading and text extraction"""

    def __init__(self):
        self.summarizer = None
        if LLM_AVAILABLE:
            print("Loading summarization model (this may take a moment)...")
            self.summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    def download_pdf(self, url: str, output_path: str) -> bool:
        """Download PDF from URL"""
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(output_path, "wb") as f:
                f.writelines(response.iter_content(chunk_size=8192))

            return True
        except Exception as e:
            print(f"  Error downloading PDF: {e}")
            return False

    def extract_text_pypdf2(self, pdf_path: str) -> str:
        """Extract text using PyPDF2"""
        text = ""
        try:
            with open(pdf_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    text += page.extract_text() + "\n"
        except Exception as e:
            print(f"  PyPDF2 extraction error: {e}")

        return text.strip()

    def extract_text_pdfplumber(self, pdf_path: str) -> str:
        """Extract text using pdfplumber (more accurate)"""
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            print(f"  pdfplumber extraction error: {e}")

        return text.strip()

    def extract_text_ocr(self, pdf_path: str) -> str:
        """Extract text using OCR (for image-based PDFs)"""
        if not OCR_AVAILABLE:
            return ""

        text = ""
        try:
            images = convert_from_path(pdf_path)
            for i, image in enumerate(images):
                page_text = pytesseract.image_to_string(image)
                text += f"--- Page {i+1} ---\n{page_text}\n"
        except Exception as e:
            print(f"  OCR extraction error: {e}")

        return text.strip()

    def extract_text(self, pdf_path: str, use_ocr: bool = False) -> str:
        """
        Extract text from PDF using multiple methods.

        Args:
            pdf_path: Path to PDF file
            use_ocr: Whether to use OCR for image-based PDFs
        """
        # Try pdfplumber first (most accurate)
        text = self.extract_text_pdfplumber(pdf_path)

        # Fallback to PyPDF2 if pdfplumber fails
        if len(text) < 100:
            text = self.extract_text_pypdf2(pdf_path)

        # Use OCR if text extraction yielded little content
        if use_ocr and len(text) < 100:
            text = self.extract_text_ocr(pdf_path)

        return text

    def check_stonybrook_content(self, text: str) -> Dict[str, any]:
        """
        Check if text contains Stony Brook-related content.

        Returns dict with:
            - found: bool
            - mentions: list of matched phrases
            - context: list of sentences containing matches
        """
        stonybrook_patterns = [
            r"stony\s*brook",
            r"suny\s*stony\s*brook",
            r"state\s*university\s*of\s*new\s*york\s*at\s*stony\s*brook",
            r"stony\s*brook\s*university",
            r"stony\s*brook\s*medicine",
            r"sbu\b",  # Common abbreviation
        ]

        mentions = []
        contexts = []

        text_lower = text.lower()

        for pattern in stonybrook_patterns:
            matches = re.finditer(pattern, text_lower, re.IGNORECASE)
            for match in matches:
                mentions.append(match.group())

                # Extract surrounding context (100 chars before and after)
                start = max(0, match.start() - 100)
                end = min(len(text), match.end() + 100)
                context = text[start:end].replace("\n", " ")
                contexts.append(f"...{context}...")

        return {
            "found": len(mentions) > 0,
            "mentions": list(set(mentions)),
            "contexts": contexts[:3],  # Limit to 3 examples
            "count": len(mentions),
        }

    def summarize_text(self, text: str, max_length: int = 150) -> Optional[str]:
        """Summarize text using language model"""
        if not self.summarizer or len(text) < 200:
            return None

        try:
            # Truncate to avoid token limits
            text_chunk = text[:1024]
            summary = self.summarizer(
                text_chunk, max_length=max_length, min_length=30, do_sample=False
            )
            return summary[0]["summary_text"]
        except Exception as e:
            print(f"  Summarization error: {e}")
            return None


def run_pipeline(email: Optional[str] = None, num_authors: int = 3, num_pubs: int = 2):
    """
    Run the complete pipeline.

    Args:
        email: Your email for OpenAlex polite pool
        num_authors: Number of authors to process
        num_pubs: Number of publications per author
    """
    print("=" * 70)
    print("STONY BROOK UNIVERSITY RESEARCH PIPELINE")
    print("=" * 70)

    # Initialize components
    api = OpenAlexAPI(email=email)
    processor = PDFProcessor()

    # Step 1: Find Stony Brook authors
    print("\n[STEP 1] Finding Stony Brook authors...")
    authors = api.find_stonybrook_authors(max_results=num_authors)

    if not authors:
        print("No authors found. Exiting.")
        return

    print(f"\nFound {len(authors)} authors.")

    # Step 2: Process each author
    for idx, author in enumerate(authors):
        print(f"\n{'=' * 70}")
        print(f"[AUTHOR {idx+1}/{len(authors)}] {author.name}")
        print(f"Works: {author.works_count} | Citations: {author.cited_by_count}")
        print(f"Affiliations: {', '.join(author.affiliations)}")
        print("=" * 70)

        # Get publications
        print("\n[STEP 2] Retrieving publications...")
        publications = api.get_author_publications(author.id, max_results=num_pubs)

        if not publications:
            print("No publications found for this author.")
            continue

        print(f"Found {len(publications)} publications.")

        # Step 3: Process each publication
        for pub_idx, pub in enumerate(publications):
            print(f"\n  --- Publication {pub_idx+1}/{len(publications)} ---")
            print(f"  Title: {pub.title}")
            print(f"  Year: {pub.publication_year}")
            print(f"  DOI: {pub.doi or 'N/A'}")

            # Check if PDF is available
            if pub.pdf_url:
                print("  PDF: Available ✓")
                print(f"  URL: {pub.pdf_url}")

                # Download and process PDF (disabled by default for testing)
                pdf_path = f"paper_{pub_idx+1}.pdf"
                if processor.download_pdf(pub.pdf_url, pdf_path):
                    print(f"  Downloaded to {pdf_path}")

                    text = processor.extract_text(pdf_path)
                    print(f"  Extracted {len(text)} characters")

                    # Check for Stony Brook content
                    check = processor.check_stonybrook_content(text)
                    print(f"  Stony Brook mentions: {check['count']}")
                    if check["found"]:
                        print(f"  Context: {check['contexts'][0]}")

                    # Summarize
                    summary = processor.summarize_text(text)
                    if summary:
                        print(f"  Summary: {summary}")

            else:
                print("  PDF: Not available ✗")

            # Show abstract if available
            if pub.abstract:
                abstract_preview = (
                    pub.abstract[:200] + "..."
                    if len(pub.abstract) > 200
                    else pub.abstract
                )
                print(f"  Abstract: {abstract_preview}")

        # Rate limiting
        time.sleep(1)

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    # Example usage
    print("Starting pipeline...")

    # Get email from environment variable, fallback to default
    email = os.getenv("EMAIL", "your.email@example.com")

    # Run with your email for faster API access
    run_pipeline(
        email=email,
        num_authors=3,
        num_pubs=2,
    )
