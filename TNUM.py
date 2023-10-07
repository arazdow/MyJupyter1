import requests
import pandas as pd
import inspect
import json
import datetime
import re

# truenumber class

class Tn(object):
    
    def __init__(self, tn = None, subject=None, property=None,
                         value=None, error=None, unit=None, tags=None, stmt = None,
                         date=None, guid=None):
        
        if(subject == None && tn != None):
            self.nuTn = Tnum.decodeNumber(tn)
            self.tn = nuTn.tn
            self.subject = nuTn.subject
            self.property = nuTn.property
            self.value = nuTn.value
            self.error = nuTn.error
            self.unit = nuTn.unit
            self.tags = nuTn.tags
            self.statement = nuTn.stmt
            self.guid = nuTn.guid
            self.date = nuTn.date
        else:
            self.tn = {}
            self.subject = subject
            self.property = property
            self.value = value
            self.error = error
            self.unit = unit
            self.tags = tags
            self.statement = stmt

            self.guid = guid
            if date is not None:
                self.date = date
            else:
                self.date = datetime.now()

    def dump(obj):
          for attr in dir(obj):
            if not attr.startswith("__"):
                print("obj.%s = %r" % (attr, getattr(obj, attr))) 

# helper class for TNs

class Tnum(object):
    nspace = ""
    nspaces = ""
    endpoint = ""
    result_cache = ""
    
    @staticmethod
    def authorize(ip="dev.truenumbers.com:8080"):
        
        Tnum.endpoint = ip.strip()

        # get list of numberspaces
        result = requests.get(f"http://{Tnum.endpoint}/v2/numberflow/numberspace")
        payload = result.json()
        if "code" in payload:
            print(payload["code"])
        else:
            Tnum.nspaces = []
            raw_nspaces = payload["numberspaces"]
            for space in raw_nspaces:
                Tnum.nspaces.append(space.split('/')[1])
            Tnum.nspace = Tnum.nspaces[0]

            print("Available spaces:", Tnum.nspaces)
            print("Defaulting to:", Tnum.getSpace())

    @staticmethod
    def createSpace(name):
        resultraw = requests.post(
            f"http://{Tnum.endpoint}/v2/numberflow/numberspace",
            json={"numberspace": name}
        )
        result = resultraw.json()
        if "code" in result: 
            print("Creating " + name + " " + result["code"])
        else:
            print("Created " + name)
            Tnum.authorize(Tnum.endpoint)

    @staticmethod
    def setSpace(name="testspace"):
        if name in Tnum.nspaces:
            Tnum.nspace = name
        else:
            raise Exception('server has no numberspace "{}"'.format(name))
        print("Numberspace set to:", Tnum.getSpace())

    @staticmethod
    def getSpace():
        return Tnum.nspace

    @staticmethod
    def query(query="* has *", SI=False, max=10, start=0):
        args = {
            "numberspace": Tnum.nspace,
            "limit": str(max),
            "offset": str(start)
        }
        payload = {"tnql": query}
        resultraw = requests.post(
            f"http://{Tnum.endpoint}/v2/numberflow/tnql",
            json=payload,
            params=args
        )
        result = resultraw.json()
        numReturned = len(result["truenumbers"])
        if numReturned > max:
            numReturned = max
        first = 0
        if numReturned > 0:
            first = start + 1

        print("Returned {} thru {} of {} results".format(
            first, start + numReturned, result["count"]))

        Tnum.result_cache = result
        return Tnum.queryResultToObjects(result, SI, max)
    ###

    @staticmethod
    def decodeNumber(tn):
        subj = tn['subject']
        prop = tn['property']
        taglist = []
        for tag in tn['tags']:
            if not tag.startswith('_'):
                taglist.append(tag)
        gid = str(tn['_id'])
        dat = tn['agent']['dateCreated'].split('T')


        valstruc = tn['value']
        stmt = tn['trueStatement']

        if valstruc['type'] == 'numeric':
            Nval = valstruc['magnitude']
            tol = valstruc['tolerance']

            if tol == 0:
                tol = None
            posuns = ""
            neguns = ""
            for unitpwr in valstruc['unitPowers']:
                if unitpwr['p'] < 0:
                    if unitpwr['p'] < -1:
                        neguns += unitpwr['u'] + '^' + str(-unitpwr['p']) + ' '
                    else:
                        neguns += unitpwr['u'] + ' '
                else:
                    if unitpwr['p'] > 1:
                        posuns += ' ' + unitpwr['u'] + '^' + str(unitpwr['p'])
                    else:
                        posuns += ' ' + unitpwr['u']
            uns = posuns
            if len(posuns) == 0 and len(neguns) > 0:
                uns = '1/' + neguns
            elif len(posuns) > 0 and len(neguns) > 0:
                uns = posuns + '/' + neguns

            if len(uns) == 0 or uns == " unity":
                uns = None

        else:
            Nval = tn['value']['value']
            tol = None
            uns = None

        return Tn(tn, subj, prop, Nval, tol, uns, taglist, stmt, dat, gid)  # return object


    @staticmethod
    def queryResultToObjects(result, SI=False, max=100):
        retList = []

        if 'truenumbers' not in result['truenumbers']:
            for tn in result['truenumbers']:
                retList.append(Tnum.decodeNumber(tn))

        else:
            count = max
            for tnList in result['truenumbers']:
                tnGroup = tnList['truenumbers']
                for tn in tnGroup:
                    retList.append(Tnum.decodeNumber(tn))
                    count -= 1
                    if count == 0:
                        break
                if count == 0:
                    break

        return retList

    ###

    # Delete tnums specified by a query
    @staticmethod
    def deleteByQuery(query=""):
        args = {
            "numberspace": Tnum.nspace
        }
        url = f"http://{Tnum.endpoint}/v2/numberflow/numbers"

        body = {
            "tnql": query
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.delete(url, params=args, json=body, headers=headers)
        result = response.json()
        numReturned = result["deletedCount"]

        plural = " match" if numReturned == 1 else " matches"
        print(f"Deleted {numReturned} {plural}")


    # Tag tnums specified by a query
    @staticmethod
    def tagByQuery(query="", adds=[], removes=[]):
        args = {
            "numberspace": Tnum.nspace
        }
        addstr = '", "'.join(adds)
        remstr = '", "'.join(removes)
        if addstr == "":
            addstr = '""'
        if remstr == "":
            remstr = '""'

        body = {
            "addTags": adds,
            "removeTags": removes,
            "tnql": query
        }

        url = f"http://{Tnum.endpoint}/v2/numberflow/numbers/tags"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.patch(url, params=args, json=body, headers=headers)
        result = response.json()
        print(f"Tags: {result['taggedCount']} added, {result['tagsRemovedCount']} removed.")


    # Utility to get attr from list
    @staticmethod
    def getAttrFromList(obs, attname, rval=None):
        ll = []
        for i in range(len(obs)):
            atv = obs[i].get(attname)
            if atv is None:
                ll.append(rval)
            else:
                ll.append(atv)
        return ll


    # Make data frame from list of tnum objects
    @staticmethod
    def objectsToDf(objs):
        len_objs = len(objs)
        subj = get_attr_from_list(objs, "subject", None)
        prop = get_attr_from_list(objs, "property", None)
        chrs = []
        nums = []
        for i in range(len_objs):
            if isinstance(objs[i], (int, float)):
                nums.append(objs[i])
                chrs.append(None)
            else:
                chrs.append(objs[i])
                nums.append(None)
        errs = get_attr_from_list(objs, "error", None)
        uns = get_attr_from_list(objs, "unit", None)
        tgs = get_attr_from_list(objs, "tags", [])
        tgschar = []
        for i in range(len_objs):
            if isinstance(tgs[i], list):
                tgschar.append(",".join(tgs[i]))
            else:
                tgschar.append(None)
        tgs = tgschar
        dat = get_attr_from_list(objs, "date", None)
        gid = get_attr_from_list(objs, "guid", None)
        df = pd.DataFrame({
            "subject": subj,
            "property": prop,
            "string.value": chrs,
            "numeric.value": nums,
            "error": errs,
            "unit": uns,
            "tags": tgs,
            "date": dat,
            "guid": gid
        })
        if isinstance(df["date"][0], (int, float)):
            df["date"] = pd.to_datetime(df["date"], origin='1970-01-01')
        return df

    @staticmethod
    def dateAsToken():
        dt = datetime.date.today().strftime("%Y-%m-%d %H-%M-%S")
        dt = dt.replace(" ", "_").replace(":", "-")
        return dt

    ###

    import re
    import json
    import requests

    @staticmethod
    def notRealString(string):
        if re.search(r"[0-9,a-z%]+", string, re.IGNORECASE):
            return False
        else:
            return True

    @staticmethod
    def makeTnumJson(subject="something", property="property", value=None,
                            numeric_error=None, unit="", tags=None, no_empty_strings=False):
        if tags is None:
            tags = []

        def escape_quotes(string):
            return string.replace('"', '\\\\"')

        numval = None
        if isinstance(value, (int, float)):
            unit_suffix = ""
            if unit and not_real_string(unit):
                unit_suffix = " " + unit
            if numeric_error is not None and not math.isnan(numeric_error):
                numval = f"{value} +/- {numeric_error}{unit_suffix}"
            else:
                numval = f"{value}{unit_suffix}"
        else:
            if no_empty_strings and not_real_string(value):
                return "{}"
            else:
                numval = value
                if not numval.startswith('"') and not re.search(r"^[0-9a-zA-Z/:\\-_]+$", numval):
                    numval = escape_quotes(numval)
                    numval = f'\\"{numval}\\"'

        tagstr = ",".join(f'"{tag}"' for tag in tags if tag and not math.isnan(tag))

        thenumber = json.dumps({
            "subject": subject,
            "property": property,
            "value": numval,
            "tags": tagstr.split(",") if tagstr else []
        })
        return thenumber

    @staticmethod
    def postFromLists(subject, property, value=None, numeric_error=None, unit=None,
                             tags=None, no_empty_strings=False):
        len_subject = len(subject)
        len_property = len(property)

        if isinstance(numeric_error, bool):
            numeric_error = [None] * len_subject
        if isinstance(unit, bool):
            unit = [None] * len_subject
        if isinstance(tags, bool):
            tags = [None] * len_subject

        all_json_nums = [
            make_tnum_json(sub, prop, val, err, u, t, no_empty_strings)
            for sub, prop, val, err, u, t in zip(subject, property, value, numeric_error, unit, tags)
        ]

        chunkcount = 0
        chunksize = 25000
        jsonnums = ""
        numnums = len(all_json_nums)

        for i in range(numnums):
            curnum = all_json_nums[i]
            chunkcount += len(curnum)
            jsonnums += curnum + ","

            if chunkcount > chunksize or i == numnums - 1:
                jsonnums = jsonnums[:-1]
                jsonnums = re.sub(",\\{\\},", ",", jsonnums)
                jsonnums = re.sub("\\{\\},", "", jsonnums)
                jsonnums = re.sub(",\\{\\}", "", jsonnums)
                jsonnums = re.sub("/iQ/", "\\\\", jsonnums)

                var_posted_json = jsonnums
                args = {"numberspace": Tnum.nspace}
                payload = f'{{"truenumbers":[{jsonnums}]}}'
                response = requests.post(
                    f"http://{Tnum.endpoint}/v2/numberflow/numbers",
                    json=payload,
                    params=args,
                    headers={"Accept": "application/json", "Content-Type": "application/json"}
                )

                if response.status_code > 199 and response.status_code < 230:
                    print(f"Posting {chunkcount} characters")
                else:
                    print(f"ERROR CODE: {response.status_code} in POST")

                chunkcount = 0
                jsonnums = ""

        print(f"posted {numnums} tnums")

    @staticmethod
    def postStatement(sentence, tags=None):
        if tags is None:
            tags = []

        payload = {
            "noReturn": True,
            "skipStore": False,
            "trueStatement": f"{sentence}",
            "tags": tags
        }
        
        pars = dict(numberspace = f"{Tnum.nspace}")

        response = requests.post(
            f"http://{Tnum.endpoint}/v2/numberflow/numbers",
            json=payload,
            params=pars
            #headers={"Accept": "application/json", "Content-Type": "application/json"}
        )

    @staticmethod
    def postObjects(objects):
        if not isinstance(objects, (list, tuple)):
            objects = [objects]

        subject = [obj["subject"] for obj in objects]
        property = [obj["property"] for obj in objects]
        error = [obj.get("error") for obj in objects]
        unit = [obj.get("unit") for obj in objects]
        tags = [obj.get("tags", []) for obj in objects]

        post_from_lists(subject, property, objects, error, unit, tags)

    @staticmethod
    def makeNumericVectorString(numvec):
        if isinstance(numvec, list):
            nvec = numvec
        else:
            nvec = [0.0] * len(numvec)

        vvals = f"vector({','.join(str(n) for n in nvec)})"
        return vvals

    @staticmethod
    def decodeNumericVector_string(nvs):
        if nvs.startswith('"vector('):
            csl = nvs[8:-2]
            Nvec = [float(n) for n in csl.split(",")]
        else:
            Nvec = []

        return Nvec
    
# Ingest group of funcs
    
    @staticmethod
    def tokenize(aString):
        tok = re.sub(r'\s+', '_', aString.strip())
        tok = re.sub(r'[^a-zA-Z0-9\'_]', '-', tok)
        return tok

    @staticmethod
    def parsePhrase(phr):
        splt = phr.split()
        splt.reverse()
        splt = ':'.join(splt)
        splt = splt.replace(':of:', '/')
        return splt

    @staticmethod
    def ingestDataFrame(df, templates):
        
        # inner functions
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
        # end inner functions

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
                postStatement(tnT, tagList)
                tnCount += 1

        print(f"{tnCount} TNs written")

    @staticmethod
    def pathLength(path):
        return len(re.findall(r'[:/]', path)) + 1

    @staticmethod
    def subPath(path, n=1):
        p = [(m.start(), m.end()) for m in re.finditer(r'[:/]', path)]
        return path[:p[n-1][0]]

