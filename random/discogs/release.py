from lxml import etree
import csv



file_name = "data/discogs_20231001_releases.xml"
# file_name = "data/sample.xml"

f = open("data/discogs_20231001_releases.csv", "w")
csv_writer = csv.DictWriter(
    f=f,
    fieldnames=["release_id", "release_title"],
    delimiter="\t",
)
csv_writer.writeheader()


def handle_artists(artists):
    ret = []
    for artist in artists:
        artist_dict = {}
        for artist_tag in artist:
            if artist_tag.tag == "id":
                artist_dict["id"] = artist_tag.text
            elif artist_tag.tag == "name":
                artist_dict["name"] = artist_tag.text
        ret.append(artist_dict)
    return ret


def fast_iter(context, func, *args, **kwargs):
    """
    http://lxml.de/parsing.html#modifying-the-tree
    Based on Liza Daly's fast_iter
    http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
    See also http://effbot.org/zone/element-iterparse.htm
    """
    for event, elem in context:
        func(elem, *args, **kwargs)
        # It's safe to call clear() here because no descendants will be
        # accessed
        elem.clear()
        # Also eliminate now-empty references from the root node to elem
        for ancestor in elem.xpath("ancestor-or-self::*"):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]
    del context


def process_event(elem):
    release = {}
    if elem.tag == "release":
        release_id = elem.get("id")
        release = {"release_id": release_id}
        for child in elem:
            tag = child.tag
            if tag == "title":
                release["release_title"] = child.text
            elif tag == "artists":
                # release["artists"] = handle_artists(child)
                pass
    if release.get("release_title") != None:
        csv_writer.writerow(release)


events = ("start",)
context = etree.iterparse(file_name, events=events)
fast_iter(context, process_event)


f.close()

# context = lxml.etree.iterparse('really-big-file.xml', tag='schedule', events=('end',))
