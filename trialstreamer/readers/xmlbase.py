#
# XML reader base class
#

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# from lxml import etree as ET

class XMLReader():
    """
    new faster version which doesn't use NLTK
    returns plain text - tokenization should be done externally
    """

    def __init__(self, filename=None, xml_string=None, xml_ET=None):

        self.filename=filename

        if filename:            
            self.parse_file(filename)
        elif xml_string:
            self.parse_string(xml_string)
        elif xml_ET:
            self.data = xml_ET
        else:
            raise TypeError('Missing XML data in any of filename, xml_string, or xml_ET')


        self.section_map = {}

    # def __getattr__(self, attr):
    #     result = self.text_filtered_all(attr)
    #     return result


    def __dir__(self):
        return self.section_map.keys()

    def parse_file(self, filename):
        self.data = ET.parse(filename)

    def parse_string(self, xml_string):
        self.data =  ET.fromstring(xml_string)

    def _ET2unicode(self, ET_instance, strip_tags=True):
        "returns unicode of elementtree contents"
        if ET_instance is not None:
            if strip_tags:
                # print "tags stripped!"
                return ' '.join([s for s in ET.tostringlist(
                                        ET_instance, method="text", encoding="unicode") if s is not None])

            else:
                return ET.tostring(ET_instance, method="xml", encoding="utf-8").decode("utf-8")
        else:
            return u""



    def _ETfind(self, element_name, ET_instance, strip_tags=True):
        "finds (first) subelement, returns unicode of contents if present, else returns None"
        subelement = ET_instance.find(element_name)
        if subelement is not None:
            return self._ET2unicode(subelement, strip_tags=strip_tags)
        else:
            return ""

    def text_filtered(self, part_id=None):
        if type(part_id) is str:
            return self._ET2unicode(self.xml_filtered(part_id=part_id)).strip()
        elif type(part_id) is list:
            return {p: self._ET2unicode(self.xml_filtered(part_id=p).strip()) for p in part_id}

    def text_filtered_all(self, part_id=None):
        if type(part_id) is str:
            return [self._ET2unicode(part).strip() for part in self.xml_filtered_all(part_id=part_id)]
        elif type(part_id) is list:
            return {p: [self._ET2unicode(part).strip() for part in self.xml_filtered_all(part_id=p)] for p in part_id}

    def text_all(self):
        output = {}
        for part_id, loc in self.section_map.iteritems():
            output[part_id] = self._ET2unicode(self.data.find(loc))
        return output

    def xml_filtered_all(self, part_id=None):
        return self.data.findall(self.section_map[part_id])

    def xml_filtered(self, part_id=None):
        return self.data.find(self.section_map[part_id])

    get = text_filtered_all # synonym
