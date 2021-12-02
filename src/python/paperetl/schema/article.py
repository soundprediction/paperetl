"""
Article module.
"""

class Article:
    """
    Article objects. Holds all metadata and text content related to an article.
    """

    # Articles schema
    ARTICLE = ("id", "source", "published", "publication", "authors", "affiliations", "affiliation", "title",
               "tags", "reference", "entry")

    # Sections schema
    SECTION = ("name", "text")

    def __init__(self, metadata, sections, source):
        """
        Stores article metadata and section content as an object.

        Args:
            metadata: article metadata
            sections: text sections
            source: article source
        """

        self.metadata = metadata
        self.sections = sections
        self.source = source

    def uid(self):
        """
        Returns the article uid.

        Returns:
            article uid
        """

        return self.metadata[0]

    def tags(self):
        """
        Returns the article tags.

        Returns:
            article tags
        """

        return self.metadata[8]

    def build(self):
        """
        Builds an article with all metadata and section content.

        Returns:
            object with full article content
        """

        # Create article
        article = dict(zip(Article.ARTICLE, self.metadata))

        # Create sections
        sections = [dict(zip(Article.SECTION, section)) for section in self.sections]

        # Add sections to article
        article["sections"] = sections

        return article
