import codecs
import json
import multiprocessing
import os
import re
import sys

import gensim
import nltk
import nltk.tokenize.punkt
import unicodedata

docstart = re.compile('<doc id="[^"]+" url="[^"]+" title="([^"]+)">')
sentok = nltk.tokenize.punkt.PunktSentenceTokenizer()


def main():
    if len(sys.argv) != 2:
        print >> sys.stderr, "usage: <input dir>"
        sys.exit(1)

    indir = sys.argv[1]

    rawfiles = [os.path.join(root, fn) for root, dirs, files in os.walk(indir)
                for fn in files if not fn.endswith(".tok.json")]

    p = multiprocessing.Pool(4)
    p.map(process, rawfiles)


# ugh, but we already know the input format and don't need to actually parse the SGML properly
# {title: [section 1, ..., section n]}
def tojson(fn):
    docs = {}
    title, section = None, ""
    with codecs.open(fn, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            if line.startswith('<doc id="'):
                m = docstart.match(line)
                if m is None:
                    raise ValueError("docstart match error on line: %s" % line)
                title = m.group(1)

                if title in docs:
                    raise ValueError("encountered already-existing title: %s" % title)
                else:
                    docs[title] = []
            elif line == ('</doc>'):
                if section != "":
                    docs[title].append(section.strip())
                    section = ""
                title = None
            elif line.startswith('<section level='):
                if section != "":
                    docs[title].append(section.strip())
                    section = ""
            else:
                if title is None:
                    raise ValueError("encountered content with no title set: %s" % line)
                else:
                    #docs[title].append(line)
                    section += " " + line

    return docs


def tokenize_string(txt, lower=True, split_sentences=False):
    txt = gensim.utils.any2unicode(txt, encoding='utf-8', errors='strict')
    txt = unicodedata.normalize('NFKD', txt)  
    #txt = re.sub(r"([.,!;?])([^\s])", r"\1 \2", txt)  # force space after punctuation

    if split_sentences:
        sents = sentok.tokenize(txt)
    else:
        sents = [txt]
        
    if lower:
        sents = [sent.lower() for sent in sents]

    return [" ".join(nltk.tokenize.word_tokenize(sent)) for sent in sents]


def process(fn):
    docs = tojson(fn)
    tokdocs = {title: [tokenize_string(section, lower=True, split_sentences=False) for section in sections]
               for title, sections in docs.iteritems()}
    
    outfn = fn + ".tok.json"
    with codecs.open(outfn, 'w', encoding='utf-8') as f:
        json.dump(tokdocs, f)

if __name__ == '__main__':
    main()
