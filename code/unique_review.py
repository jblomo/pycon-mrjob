from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import re

WORD_RE = re.compile(r"[\w']+")

class UniqueReview(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def extract_words(self, _, record):
        """Take in a record, filter by type=review, yield <word, review_id>"""
        if record['type'] == 'review':
            for word in WORD_RE.findall(record['text']):
                yield [word.lower(), record['review_id']]


    def count_reviews(self, word, review_ids):
        """Count the number of reviews a word has appeared in.  If it is a
        unique word (ie it has only been used in 1 review), output that review
        and 1 (the number of words that were unique)."""

        unique_reviews = set(review_ids)  # set() uniques an iterator
        if len(unique_reviews) == 1:
            yield [unique_reviews.pop(), 1]

    def count_unique_words(self, review_id, unique_word_counts):
        """Output the number of unique words for a given review_id"""
        yield [review_id, sum(unique_word_counts)]

    def steps(self):
        """TODO: Document what you expect each mapper and reducer to produce:
        mapper1: <line, record> => <key, value>
        reducer1: <key, [values]>
        mapper2: ...
        """
        return [self.mr(self.extract_words, self.count_reviews),
                self.mr(reducer=self.count_unique_words)]

if __name__ == '__main__':
    UniqueReview.run()
