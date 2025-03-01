import fitz
from google.cloud import storage
import vertexai
import textwrap
from vertexai.generative_models import GenerativeModel, SafetySetting
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import io
import streamlit as st

# Initialize Vertex AI and GCS
vertexai.init(
        project = "pdf-translate-452308",
        location = "us-east1"
    )

client = storage.Client(project = "pdf-translate-452308")

def download_pdf_text(bucket_name, file_name):
    """Download a PDF from GCS and extract text."""
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    pdf_content = blob.download_as_string()

    # Extract text from PDF
    pdf_text = ""
    with fitz.open("pdf", pdf_content) as pdf:
        for page in pdf:
            pdf_text += page.get_text()
    return pdf_text

def translate_text(text):
    """Translates the extracted text to French using Vertex AI."""
    model = GenerativeModel("gemini-1.5-flash-001")
    responses = model.generate_content(
        [f"Translate the following text to French: {text}"],
        generation_config = {
            "candidate_count": 1,
            "max_output_tokens": 8192,
            "temperature": 0,
            "top_p": 1,
            "top_k": 1,
        },
        safety_settings = [
            SafetySetting(
                category = SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold = SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
        ]
    )

    translated_text = ""
    for candidate in responses.candidates:
        for part in candidate.content.parts:
            translated_text += part.text
    return translated_text

def create_pdf(content):
    """Create a properly formatted PDF from the translated text with word wrapping."""
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=letter)
    
    # Define margins and line spacing
    text_x = 40  # Left margin
    text_y = 750  # Top margin
    max_width = 700  # Maximum width before wrapping (approx. page width)
    line_height = 14  # Space between lines
    
    can.setFont("Helvetica", 12)

    # Wrap text to fit within the max width
    wrapped_lines = []
    for line in content.split("\n"):
        wrapped_lines.extend(textwrap.wrap(line, width=80))  # Adjust width as needed

    # Add wrapped text to the PDF
    for line in wrapped_lines:
        if text_y <= 40:  # Prevent text from going beyond the bottom margin
            can.showPage()  # Create a new page
            can.setFont("Helvetica", 12)
            text_y = 750  # Reset text position for the new page

        can.drawString(text_x, text_y, line)
        text_y -= line_height  # Move to the next line

    can.save()
    packet.seek(0)
    return packet

def upload_pdf_to_gcs(bucket_name, pdf_content, output_filename):
    """Upload the translated PDF to GCS."""
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(output_filename)
    blob.upload_from_file(pdf_content, content_type="application/pdf")
    print(f"PDF uploaded to gs://{bucket_name}/{output_filename}")

# Processing all the PDF's in source bucket
def process_all_pdfs(source_bucket_name, destination_bucket_name):
    """Process multiple PDFs from the source bucket."""
    bucket = client.get_bucket(source_bucket_name)
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        if blob.name.lower().endswith(".pdf"):
            print(f"Processing: {blob.name}")
            pdf_text = download_pdf_text(source_bucket_name, blob.name)
            translated_text = translate_text(pdf_text)
            output_pdf = create_pdf(translated_text)
            translated_filename = f"translated_{blob.name}"
            upload_pdf_to_gcs(destination_bucket_name, output_pdf, translated_filename)

# Streamlit UI
st.title("PDF Translation Service")

if st.button("Clear Existing Files"):
    # Delete existing files in both source and destination buckets
    source_bucket = client.get_bucket("aa-translate-source")
    for blob in source_bucket.list_blobs():
        blob.delete()
    destination_bucket = client.get_bucket("aa-translate-destination")
    for blob in destination_bucket.list_blobs():
        blob.delete()
    st.success("Existing files cleared.")

uploaded_files = st.file_uploader("Upload PDFs", type=["pdf"], accept_multiple_files=True)

if uploaded_files:
    for uploaded_file in uploaded_files:
        bucket = client.get_bucket("aa-translate-source")
        blob = bucket.blob(uploaded_file.name)
        blob.upload_from_file(uploaded_file, content_type="application/pdf")
    st.success("Files uploaded successfully!")

if st.button("Process PDFs"):
    process_all_pdfs("aa-translate-source", "aa-translate-destination")
    st.success("PDFs translated and uploaded to destination bucket.")
