"""Find the titles of the top visited Vroots.

This program will take a CSV data file and output lines of

    Vroot Title

To run:

    python count_titles.py anonymous-msweb.data
"""

from mrjob.job import MRJob
import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class CountTitles(MRJob):

    def mapper(self, line_no, line):
        """Extracts the Vroot that was visited"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield cell[1], ('V', 1)
                  # How to "tag" this value for a given Key?
        elif cell[0] == 'A':
            yield cell[1], ('A', cell[3])
                  # How to "tag" this value for a given Key?

    def reducer(self, vroot, visit_counts_and_title):
        """Sumarizes the visit counts by adding them together.  Extracts title,
        sums visit counts, returns both."""
        total = 0
        title = ''

        for value in visit_counts_and_title:
            if value[0] == 'V':
                total += value[1]
            elif value[0] == 'A':
                title = value[1]

        yield title, total
        
if __name__ == '__main__':
    CountTitles.run()
