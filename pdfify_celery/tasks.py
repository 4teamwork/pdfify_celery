from celery import Celery
from celeryconfig import PDFTOOLS_PICKUP_FOLDER, PDFTOOLS_OUTPUT_FOLDER
from time import sleep
import requests
import os.path
import logging

CHUNK_SIZE = 65536
OUTPUT_PDF_PREFIX = 'Job-01CF701A-00000001_'
logger = logging.getLogger('pdfify_celery')
app = Celery('tasks')
app.config_from_object('celeryconfig')


def uuid_filename(filename, uuid):
    base, ext = os.path.splitext(filename)
    return uuid + ext


@app.task
def convert(url, uuid, filename, content_type):
    """Convert the document at the given ``url`` to PDF."""
    chain = (fetch_document.s(url, uuid, filename)
             | wait_for_pdf.s(filename)
             | upload_pdf.s(url, uuid, filename))
    chain()


@app.task
def fetch_document(url, uuid, filename):
    doc_filename = os.path.join(PDFTOOLS_PICKUP_FOLDER,
                                uuid_filename(filename, uuid))
    req = requests.get(url, stream=True)
    if req.status_code == 200:
        with open(doc_filename, 'wb') as fd:
            for chunk in req.iter_content(CHUNK_SIZE):
                fd.write(chunk)


@app.task
def wait_for_pdf(uuid, filename):
    pdf_filename = (os.path.basename(uuid_filename(filename, uuid))
                    + os.path.extsep + 'pdf')

    # Simple approach for watching directory for changes
    # More advanced methods can be found here:
    # http://timgolden.me.uk/python/win32_how_do_i/watch_directory_for_changes.html
    while True:
        filenames = os.listdir(PDFTOOLS_OUTPUT_FOLDER)
        for fn in filenames:
            if pdf_filename == fn[len(OUTPUT_PDF_PREFIX):]:
                return
        sleep(10)


@app.task
def upload_pdf(url, uuid, filename):
    pdf_filename = os.path.join(
        PDFTOOLS_OUTPUT_FOLDER,
        (os.path.basename(uuid_filename(filename, uuid))
         + os.path.extsep + 'pdf')
    )

    with open(pdf_filename, 'rb') as fd:
        req = requests.post(url, data=fd)

    if req.status_code != 200:
        logger.warning('PDF upload failed')
