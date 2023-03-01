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

CLEANR = re.compile("<.*?>")


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
            title = JAMA.get(entry, "article-title")
            published = JAMA.get(entry, "pub-date")
            try:
                yr = published[-4:]
                mo = published[3:-4]
                day = published[:2]
                published = parser.parse(f"{yr}-{mo}-{day}")
            except ArithmeticError:
                published = None
            updated = None

            # Derive uid
            uid = hashlib.sha1(reference.encode("utf-8")).hexdigest()

            # Get journal reference
            journal = JAMA.get(entry, "journal-title")

            # Get authors
            authors, affiliations = JAMA.authors(
                entry.find_all("contrib-group"), entry.find_all("aff")
            )

            # Get tags
            tags = []
            for cats in entry.find_all("article-categories"):
                tags += cats.find_all("subject")

            tags = [re.sub(CLEANR, "", x.text) for x in tags if x is not None]
            tags = "; ".join(["JAMA"] + tags)
            abstract = entry.abstract
            body = entry.body
            # Transform section text
            sections = JAMA.sections(title, abstract=abstract, body=body)

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
    def sections(title, abstract, body):
        """
        Gets a list of sections for this article. This method supports the following three abstract formats:
           - Raw text
           - HTML formatted text
           - Abstract text parsed into section named elements

        All three formats return sections that are tokenized into sentences.

        Args:
            article: article element
            title: title string

        Returns:
            list of sections
        """

        sections = [("TITLE", title)] if title else []
        try:
            for sec in abstract.find_all("sec"):
                sections += [
                    (
                        "ABSTRACT\\" + sec.title.text,
                        " ".join([x.text for x in sec.find_all("p")]),
                    )
                ]
        except AttributeError:
            pass

        
        for sec in body.find_all("sec", recursive=False):
            subsections = sec.find_all("sec")
            if len(subsections) == 0:
                sections += [
                    (
                        sec.title.text.upper(),
                        x
                    )
                    for x in sent_tokenize(
                            Text.transform(
                                " ".join([x.text for x in sec.find_all("p")])
                            )
                        )
                    
                ]
            else:
                for ssec in subsections:
                    sections += [
                        (
                            f"{sec.title.text.upper()}\\{ssec.title.text.upper()}",
                            x
                        )
                        for x in sent_tokenize(
                                Text.transform(
                                    " ".join([x.text for x in ssec.find_all("p")])
                                )
                            )
                        
                    ]

        return sections
