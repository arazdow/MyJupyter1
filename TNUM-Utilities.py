import re

import pandas as pd
# TNUM Utilities

def tokenize(aString):
    tok = re.sub(r'\s+', '_', aString.strip())
    tok = re.sub(r'[^a-zA-Z0-9\'_]', '-', tok)
    return tok

def tnum_parsePhrase(phr):
    splt = phr.split()
    splt.reverse()
    splt = ':'.join(splt)
    splt = splt.replace(':of:', '/')
    return splt

def tnum_loadLibs():
    import tnum
    import jsonlite
    import httr
    import lubridate
    import stringr
    import knitr

def tnum_ingestDataFrame(df, templates):
    tkn = lambda val: re.sub(r'\s+', '_', val.strip()) if not pd.isnull(val) and isinstance(val, str) else val

    dateTkn = lambda: tkn(str(date()))

    def doTemplates(macros, tmplt, theRow):
        for macro in macros:
            fn = re.search(r'\$(.*)\(', macro)
            if fn is not None:
                fn = fn.group(1)
            else:
                fn = ""

            mac = re.search(r'\((.*)\)', macro)
            if mac is not None:
                mac = mac.group(1)
            else:
                mac = ""

            if mac is not None and len(mac) > 0:
                vl = theRow[mac]
            else:
                vl = ""

            if len(fn) > 0:
                if mac is not None and len(mac) > 0:
                    theExp = f"{fn}(vl)"
                else:
                    theExp = f"{fn}()"
                vl = eval(theExp)
            tmplt = tmplt.replace(macro, str(vl))
        return tmplt

    theFile = None
    dfRows = df.shape[0]
    dfCols = df.shape[1]
    dfTemps = len(templates)
    doCache = False
    tnCount = 0

    for i in range(dfRows):
        for pair in templates:
            tnT = pair[0]
            tagT = pair[1]

            macros = re.findall(r'\$[a-zA-Z0-9_]*\([a-zA-Z0-9_\s]*\)', tnT)
            tnT = doTemplates(macros, tnT, df.iloc[i])

            macros = re.findall(r'\$[a-zA-Z0-9_]*\([a-zA-Z0-9_]*\)', tagT)
            tagT = doTemplates(macros, tagT, df.iloc[i])
            tagList = []
            if len(tagT) > 0:
                tagList = [tag.strip() for tag in tagT.split(",")]
            tnum_postStatement(tnT, tagList)
            tnCount += 1

    print(f"{tnCount} TNs written")


def tnum_pathLength(path):
    return len(re.findall(r'[:/]', path)) + 1


def tnum_subPath(path, n=1):
    p = [(m.start(), m.end()) for m in re.finditer(r'[:/]', path)]
    return path[:p[n-1][0]]
