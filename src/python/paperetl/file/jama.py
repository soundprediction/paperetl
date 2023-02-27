"""
JAMAiv XML processing module
"""

import hashlib
import re

from bs4 import BeautifulSoup
from dateutil import parser
from nltk.tokenize import sent_tokenize

from ..schema.article import Article
from ..text import Text


class JAMA:
    """
    Methods to transform JAMA XML into article objects.
    """

    @staticmethod
    def parse(stream, source):
        """
        Parses a XML datastream and yields processed articles.

        Args:
            stream: handle to input data stream
            source: text string describing stream source, can be None
            config: path to config directory
        """

        # Parse XML
        soup = BeautifulSoup(stream, features="xml")
        
        # Each file is a single article

        for entry in soup.find_all("article"):
            reference = JAMA.get(entry, "article-id")
            title = JAMA.get(entry, "title")
            published = JAMA.get(entry, "pub-date")
            updated = None

            # Derive uid
            uid = hashlib.sha1(reference.encode("utf-8")).hexdigest()

            # Get journal reference
            journal = JAMA.get(entry, "journal-title")

            # Get authors
            authors, affiliations = JAMA.authors(entry.find_all("contrib-group"), entry.find_all("aff"))

            # Get tags
            tags = "; ".join(
                ["JAMA"]
                + [category.get("term") for category in entry.find_all("category")]
            )

            # Transform section text
            sections = JAMA.sections(title, JAMA.get(entry, "summary"))

            # Article metadata - id, source, published, publication, authors, affiliations, affiliation, title,
            #                    tags, reference, entry date
            metadata = (
                uid,
                source,
                published,
                journal,
                authors,
                affiliations,
                "",
                title,
                tags,
                reference,
                updated,
            )

            yield Article(metadata, sections)

    @staticmethod
    def get(element, path):
        """
        Finds the first matching path in element and returns the element text.

        Args:
            element: XML element
            path: path expression

        Returns:
            string
        """

        element = element.find(path)
        return JAMA.clean(element.text) if element else None

    @staticmethod
    def clean(text):
        """
        Removes newlines and extra spacing from text.

        Args:
            text: text to clean

        Returns:
            clean text
        """

        # Remove newlines and cleanup spacing
        text = text.replace("\n", " ")
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def authors(elements, affiliations=None):
        """
        Parses authors and associated affiliations from the article.

        Args:
            elements: authors elements

        Returns:
            (semicolon separated list of authors, semicolon separated list of affiliations, primary affiliation)
        """

        authors = []
        affiliations = [] if affiliations is None else affiliations
        CLEANR = re.compile('<.*?>') 
        for group in elements:
            # Create authors as lastname, firstname
            for author in group.find_all("contrib"):
                name = ", ".join([re.sub(CLEANR, "", str(j)) for j in author.next()])
                authors.append(name)

            # Add affiliations
        affiliations = [x.contents[1] for x in affiliations]

        return (
            "; ".join(authors),
            "; ".join(affiliations),
        )

    @staticmethod
    def sections(title, text):
        """
        Gets a list of sections for this article.

        Args:
            title: title string
            text: summary text

        Returns:
            list of sections
        """

        # Add title
        sections = [("TITLE", title)]

        # Transform and clean text
        text = Text.transform(text)

        # Split text into sentences, transform text and add to sections
        sections.extend([("ABSTRACT", x) for x in sent_tokenize(text)])

        return sections
