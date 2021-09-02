#
#   Pubmed reader
#
from glob import glob
import codecs
import re
from trialstreamer.readers.xmlbase import XMLReader
import xml.etree.cElementTree as ET


def list_bounds(input_list, index, boundary):
    """
    returns indexed word with words surrounding
    useful function in many places
    for displaying collocations
    """
    index_lower = index - boundary
    if index_lower < 0:
        index_lower = 0

    index_upper = index + boundary + 1
    if index_upper > len(input_list):
        index_upper = len(input_list)

    return input_list[index_lower:index_upper]


class NLMCorpusReader(XMLReader):
    pass


class PMCCorpusReader(NLMCorpusReader):
    #
    # not fully functioning yet - nxml files are not really valid xml - they contain HTML within some fields
    #
    def __init__(self, filename=None, xml_string=None, xml_ET=None):
        NLMCorpusReader.__init__(self, filename=filename, xml_string=xml_string, xml_ET=xml_ET)
        self.section_map["title"] = 'front/article-meta/title-group/article-title'
        self.section_map["abstract"] = 'front/article-meta/abstract'

    def title(self):
        title = self.data.find(self.section_map["title"])
        return self._ET2unicode(title).strip()

    def abstract(self):
        abstract = self.data.find(self.section_map["abstract"])
        return self._ET2unicode(abstract)


class PubmedCorpusReader(NLMCorpusReader):

    def __init__(self, filename=None, xml_string=None, xml_ET=None):
        NLMCorpusReader.__init__(self, filename=filename, xml_string=xml_string, xml_ET=xml_ET)
        self.section_map["title"] = 'Article/ArticleTitle'
        self.section_map["vernacular_title"] = 'Article/VernacularTitle'
        self.section_map["abstract"] = 'Article/Abstract'
        self.section_map["linked_ids"] = 'OtherID'
        self.section_map["pmid"] = 'PMID'
        self.section_map["mesh"] = 'MeshHeadingList/MeshHeading/DescriptorName'
        self.section_map["language"] = 'Article/Language'
        self.section_map["journal"] = 'Article/Journal/Title'
        self.section_map["journal_abbrv"] = 'Article/Journal/ISOAbbreviation'
        self.section_map["issue"] = 'Article/Journal/JournalIssue/Issue'
        self.section_map["volume"] = 'Article/Journal/JournalIssue/Volume'
        self.section_map["ptyp"] = 'Article/PublicationTypeList/PublicationType'
        self.section_map["year"] = 'Article/Journal/JournalIssue/PubDate/Year'
        self.section_map["medlinedate"] = 'Article/Journal/JournalIssue/PubDate/MedlineDate'
        self.section_map["month"] = 'Article/Journal/JournalIssue/PubDate/Month'
        self.section_map["pages"] = 'Article/Pagination/MedlinePgn'
        self.section_map["registry_ids"] = 'Article/DataBankList/DataBank/AccessionNumberList/AccessionNumber'
        self.section_map["chemical_list"] = 'ChemicalList/Chemical/NameOfSubstance'

    def title(self):
        title = self.data.find(self.section_map["title"])
        title_try1 = self._ET2unicode(title).strip()
        if title_try1 != '' and title_try1 != '[Not Available].':
            return title_try1
        else:
            title = self.data.find(self.section_map["vernacular_title"])
            title_try2 = self._ET2unicode(title).strip()
            if title_try2 != '' and title_try2 != '[Not Available].':
                return title_try2
            else:
                return ''

    def abstract(self):
        abstract_sections = self.data.findall('Article/Abstract/AbstractText')
        abstract_text = []
        for sec in abstract_sections:
            header = sec.get('Label', "_UNSTRUCTURED")
            abstract_text.append({"header": header,
                                  "text": self._ET2unicode(sec).strip()})

        return abstract_text

    def abstract_plaintext(self):
        out = []
        for sec in self.abstract():
            if sec["header"] != "_UNSTRUCTURED":
                out.append(sec["header"])
                out.append("\n")
            out.append(sec["text"])

        return "\n".join(out)

    def authors(self):
        output = []
        author_list = self.data.findall('Article/AuthorList/Author')
        for author_el in author_list:
            output.append({"Initials": self._ET2unicode(author_el.find('Initials')).strip(),
                      "LastName": self._ET2unicode(author_el.find('LastName')).strip(),
                        "ForeName":self._ET2unicode(author_el.find('ForeName')).strip(),
                        "Affiliation":self._ET2unicode(author_el.find('AffiliationInfo/Affiliation')).strip()})
        return output

    def doi(self):
        eids = self.data.findall('Article/ELocationID')
        return [e.text for e in eids if e.attrib.get('EIdType')=='doi']

    def is_pmc_linked(self):
        els = self.data.findall('OtherID')
        if len(els) > 0:
            for el in els:
                text = self._ET2unicode(el)
                result = re.match("PMC[0-9]+", text)
                if result is not None:
                    return result.group(0)
        return None

    def parse_pages(self, page_string):
        parts = page_string.split('-')
        if len(parts) == 2:
            l0, l1 = len(parts[0]), len(parts[1])
            page_to = parts[0][:l0-l1] + parts[1]
            page_from = parts[0]
            return {"page_from":page_from, "page_to":page_to}
        elif len(parts)==1:
            page_from, page_to = parts[0], parts[0]
            return {"page_from":page_from, "page_to":page_to}
        else:
            return {}

    def yr_proc(self, raw_date):
        m = re.search(r"\b(19|20)\d{2}\b", raw_date)
        if m:
            return m.group(0)
        else:
            return None

    def year(self):
        yr_try1 = self.text_filtered('year')
        if yr_try1 != '':
            return yr_try1
        else:
            yr_try2 = self.yr_proc(self.text_filtered('medlinedate'))
            return yr_try2

    def to_dict(self):
        out = {"pmid": self.text_filtered('pmid'),
               "status": self.status(),
               "indexing_method": self.indexing_method(),
               "title": self.title(),
               "abstract": self.abstract(),
               "abstract_plaintext": self.abstract_plaintext(),
               "authors": self.authors(),
               "journal": self.text_filtered('journal'),
               "journal_abbrv": self.text_filtered('journal_abbrv'),
               "year": self.year(),
               "mesh": self.text_filtered_all('mesh'),
               "month": self.text_filtered('month'),
               "volume": self.text_filtered('volume'),
               "issue": self.text_filtered('issue'),
               "pages": self.parse_pages(self.text_filtered('pages')),
               "ptyp": self.text_filtered_all('ptyp'),
               "registry_ids": self.text_filtered_all('registry_ids'),
               "dois": self.doi()}
        return out

    def status(self):
        return self.data.attrib.get('Status')

    def indexing_method(self):
        return self.data.attrib.get('IndexingMethod', 'Human')
