import requests
import os
import json
from datetime import datetime
from utils.logger import logging, CustomException
import sys
import unicodedata
from llama_index.core import Document, VectorStoreIndex, Settings
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.llms.ollama import Ollama
from llama_index.vector_stores.chroma import ChromaVectorStore
import chromadb
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import PyPDF2
import re

class UPME:
    def __init__(self, url: str = "https://www1.upme.gov.co/Entornoinstitucional/Biblioteca-juridica/Paginas/Resoluciones-UPME-Energia-electrica.aspx"):
        self.data_path = "src/creg/data"
        self.url = url
        self.embedding_model = OllamaEmbedding(
            model_name="mxbai-embed-large",
            base_url="http://localhost:11434",
            ollama_additional_kwargs={"mirostat": 0},
        )
        Settings.embed_model = self.embedding_model
        self.llm = Ollama(model="llama3.2:3b", request_timeout=120.0)
        Settings.llm = self.llm

    def set_up_driver(self) -> webdriver.Chrome:
        """
        Sets up the Chrome driver for Selenium

        Returns:
            driver (webdriver.Chrome): Chrome driver
        """
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")  # Run in headless mode (optional)
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("start-maximized")
            options.add_argument("disable-infobars")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            )
            driver = webdriver.Chrome(options=options)
            return driver
        except Exception as e:
            logging.error(f"Error setting up driver: {CustomException(e, sys)}")
            return None

    def get_pdf_links(self, driver) -> list:
        """
        Gets the PDF links from the CREG website

        Args:
            driver (webdriver.Chrome): Chrome driver
        
        Returns:
            pdf_links (list): List of PDF links
        """
        try:
            driver.get(self.url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            pdf_links = [
                element.get_attribute("href")
                for element in driver.find_elements(
                    By.CSS_SELECTOR, "a.ms-srch-item-link"
                )
            ]
            pdf_links = [link for link in pdf_links if link.endswith(".pdf")]
            return pdf_links
        except Exception as e:
            logging.error(f"Error detecting file links: {CustomException(e, sys)}")
            return []

    def quit_driver(self, driver: webdriver.Chrome):
        """
        Quits the Chrome driver

        Args:
            driver (webdriver.Chrome): Chrome driver
        """
        try:
            driver.quit()
        except Exception as e:
            logging.error(f"Error quitting driver: {CustomException(e, sys)}")

    def download_documents(self, link_list: list) -> bool:
        """
        Downloads the documents from the UPME website

        Args:
            documents (list): List of dictionaries containing the title and url of the document

        Returns:
            sucess: True if the documents were downloaded successfully, False otherwise
        """
        try:
            for link in link_list:
                filename = link[-26:]
                filename = filename.replace("/", "-")
                file_path = os.path.join(self.data_path, filename)
                response = requests.get(link, timeout=10)
                with open(file_path, "wb") as file:
                    file.write(response.content)

            logging.info(f"Downloaded latest 10 resolutions from CREG")
            return True
        except Exception as e:
            logging.error(f"Error downloading documents: {CustomException(e, sys)}")
            return False

    def remove_accents(self, text:str) -> str:
        """
        Removes accents from the text

        Args:
            text (str): Text to remove accents from

        Returns:
            text (str): Text without accents
        """
        return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """ Extracts text from a PDF file

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            str: Full text extracted from the PDF file
        """
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            full_text = [p.extract_text() for p in reader.pages]
            full_text = [p for p in full_text if p]  # Remove empty paragraphs
            full_text = [self.remove_accents(p) for p in full_text]

        return "\n".join(full_text)

    def extract_metadata(self, text: str) -> dict:
        """ Extracts metadata from the text

        Args:
            text (str): Text to extract metadata from

        Returns:
            dict: Dictionary with the metadata extracted from the text
        """
        resolution_number = None
        date = None
        concept = None

        # Buscar número de resolución (ejemplo: RESOLUCIÓN No. 000457 de 2024)
        res_match = re.search(r"RESOLUCIÓN\s+No\.\s+\d+\s+de\s+\d{4}", text, re.IGNORECASE)
        if res_match:
            resolution_number = res_match.group()
            resolution_number = self.remove_accents(resolution_number)

        # Buscar fecha (ejemplo: 19-06-2024)
        date_match = re.search(r"\d{2}-\d{2}-\d{4}", text)
        if date_match:
            date = date_match.group()
            date = datetime.strptime(date, "%d-%m-%Y").strftime("%Y-%m-%d")

        # Buscar concepto (usando comillas y heurística)
        concept_match = re.search(r"“([^”]+)”", text)
        if concept_match:
            concept = concept_match.group(1)
            concept = self.remove_accents(concept)

        return {
            "name": resolution_number,
            "resolution_date": date,
            "concept": concept,
            "process_date": datetime.now().strftime("%Y-%m-%d"),
        }

    def process_documents(self, documents: list) -> bool:
        """ Processes the documents downloaded from the CREG website and get text and metadata

        Args:
            documents (list): List of dictionaries containing the title and url of the document

        Returns:
            bool: True if the documents were processed successfully, False otherwise
        """
        resolutions = []
        for resolution in documents:
            try:
                pdf_path = f"{self.data_path}/{resolution}"
                full_text = self.extract_text_from_pdf(pdf_path)
                metadata = self.extract_metadata(full_text)
                metadata["full_text"] = full_text
                resolutions.append(metadata)
                # delete file
                os.remove(pdf_path)
                logging.info(f"Processed resolution: {metadata['name']}")    

            except Exception as e:
                logging.error(f"Error processing documents: {CustomException(e, sys)}")
                os.rename(f"{self.data_path}/{resolution}", f"{self.data_path}/to_check/{resolution}")
                continue

        if resolutions:
            file_name = f"resolutions_processed.json"
            folder = f"{self.data_path}/processed"
            file_path = os.path.join(folder, file_name)
            with open(file_path, "w") as file:
                json.dump(resolutions, file, indent=4, ensure_ascii=False)
            return True
        else:
            logging.error(f"No resolutions were processed")
            return False

    def model_resolution_doc(self):
        """
        Model the resolution document

        Args:
            None

        Returns:
            documents (list): List of documents modeled with llama index
        """
        try:
            file_path = f"{self.data_path}/processed/resolutions_processed.json"
            with open(file_path, "r", encoding="utf-8") as f:
                resolutions = json.load(f)
            documents = [
                Document(
                    text=resolution["full_text"],
                    metadata={
                        "name": resolution["name"],
                        "date": resolution["resolution_date"],
                        "concept": resolution["concept"],
                    },
                )
                for resolution in resolutions
            ]
            return documents
        except Exception as e:
            logging.error(f"Error modeling resolution document: {CustomException(e, sys)}")
            return []

    def get_upme_vector_store(self, documents: list) -> VectorStoreIndex:
        """
        Get the vector store for the UPME model

        Args:
            documents (list): List of documents modeled with llama index

        Returns:
            index (VectorStoreIndex): Vector store index for the UPME model
        """
        try:
            db2 = chromadb.PersistentClient(path="chroma_db")
            chroma_collection = db2.get_or_create_collection("upme_index")
            vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            index = VectorStoreIndex.from_vector_store(
                vector_store,
                embed_model=self.embedding_model,
            )
            return index
        except Exception as e:
            logging.error(f"Error getting UPME vector store: {CustomException(e, sys)}")
            return None

    def get_query_engine(self, index: VectorStoreIndex):
        """
        Get the query engine for the UPME model

        Args:
            index (VectorStoreIndex): Vector store index for the UPME model

        Returns:
            query_engine (QueryEngine): Query engine for the UPME model
        """
        try:
            query_engine = index.as_query_engine()
            return query_engine
        except Exception as e:
            logging.error(f"Error getting query engine: {CustomException(e, sys)}")
            return None
