import requests
import pandas as pd
import inspect

# Vars local to this file
tnum_env = {}

# truenumber class

class Tn(object):
  def __init__(self, subject="something", property="some-property",
                     value=None, error=None, unit=None, tags=None, stmt = "",
                     date=None, guid=None):

    self.subject = subject
    self.property = property
    self.value = value
    self.error = error
    self.unit = unit
    self.tags = tags
    self.stmt = stmt

    self.guid = guid
    if date is not None:
        self.date = date
    else:
        self.date = datetime.now()
        
  def dump(obj):
      for attr in dir(obj):
        if not attr.startswith("__"):
            print("obj.%s = %r" % (attr, getattr(obj, attr))) 


def tnum_authorize(ip="dev.truenumbers.com"):
    ip1 = ip
    if ":" not in ip1:
        ip1 = ip1.strip() + ":80"
    tnum_env["tnum.var.ip"] = ip1

    # get list of numberspaces
    result = requests.get(f"http://{ip1}/v2/numberflow/numberspace")
    payload = result.json()
    if "code" in payload:
        print(payload["code"])
    else:
        nspaces = []
        raw_nspaces = payload["numberspaces"]
        for space in raw_nspaces:
            nspaces.append(space.split('/')[1])
        print(nspaces)
        tnum_env["tnum.var.nspace"] = nspaces[0]
        tnum_env["tnum.var.nspaces"] = nspaces

        tnum_setSpace(nspaces[0])
        print("Available spaces:", nspaces)
        print("Defaulting to:", tnum_getSpace())

def tnum_createSpace(name):
    result = requests.post(
        f"http://{tnum_env['tnum.var.ip']}/v2/numberflow/numberspace",
        json={"numberspace": name}
    )
    return result.json()

def tnum_setSpace(name="testspace"):
    if name in tnum_env["tnum.var.nspaces"]:
        tnum_env["tnum.var.nspace"] = name
    else:
        raise Exception('server has no numberspace "{}"'.format(name))
    print("Numberspace set to:", tnum_getSpace())
    
def tnum_getSpace():
    return tnum_env["tnum.var.nspace"]

def tnum_query(query="* has *", SI=False, max=10, start=0):
    args = {
        "numberspace": tnum_env["tnum.var.nspace"],
        "limit": str(max),
        "offset": str(start)
    }
    payload = {"tnql": query}
    resultraw = requests.post(
        f"http://{tnum_env['tnum.var.ip']}/v2/numberflow/tnql",
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

    tnum_env["tnum.var.result"] = result
    return tnum_queryResultToObjects(result, SI, max)
###

def decodenumber(tn):
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

    return Tn(subj, prop, Nval, tol, uns, taglist, stmt, dat, gid)  # return object


def tnum_queryResultToObjects(result, SI=False, max=100):
    retList = []

    if 'truenumbers' not in result['truenumbers']:
        for tn in result['truenumbers']:
            retList.append(decodenumber(tn))

    else:
        count = max
        for tnList in result['truenumbers']:
            tnGroup = tnList['truenumbers']
            for tn in tnGroup:
                retList.append(decodenumber(tn))
                count -= 1
                if count == 0:
                    break
            if count == 0:
                break

    return retList

###

import requests
import json
import datetime

# Delete tnums specified by a query
def tnum_delete_by_query(query=""):
    args = {
        "numberspace": tnum.env.tnum_var.nspace
    }
    url = f"http://{tnum.env.tnum_var.ip}/v2/numberflow/numbers"

    body = {
        "tnql": query
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    response = requests.delete(url, params=args, json=body, headers=headers)
    result = response.json()
    numReturned = len(result["deletedCount"])

    plural = " match" if numReturned == 1 else " matches"
    print(f"deleted {numReturned} {plural}")


# Tag tnums specified by a query
def tnum_tag_by_query(query="", adds=[], removes=[]):
    args = {
        "numberspace": tnum.env.tnum_var.nspace
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

    url = f"http://{tnum.env.tnum_var.ip}/v2/numberflow/numbers/tags"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    response = requests.patch(url, params=args, json=body, headers=headers)
    result = response.json()
    print(f"Tags: {result['taggedCount']} added, {result['tagsRemovedCount']} removed.")


# Utility to get attr from list
def tnum_get_attr_from_list(obs, attname, rval=None):
    ll = []
    for i in range(len(obs)):
        atv = obs[i].get(attname)
        if atv is None:
            ll.append(rval)
        else:
            ll.append(atv)
    return ll


# Make data frame from list of tnum objects
def tnum_objects_to_df(objs):
    len_objs = len(objs)
    subj = tnum_get_attr_from_list(objs, "subject", None)
    prop = tnum_get_attr_from_list(objs, "property", None)
    chrs = []
    nums = []
    for i in range(len_objs):
        if isinstance(objs[i], (int, float)):
            nums.append(objs[i])
            chrs.append(None)
        else:
            chrs.append(objs[i])
            nums.append(None)
    errs = tnum_get_attr_from_list(objs, "error", None)
    uns = tnum_get_attr_from_list(objs, "unit", None)
    tgs = tnum_get_attr_from_list(objs, "tags", [])
    tgschar = []
    for i in range(len_objs):
        if isinstance(tgs[i], list):
            tgschar.append(",".join(tgs[i]))
        else:
            tgschar.append(None)
    tgs = tgschar
    dat = tnum_get_attr_from_list(objs, "date", None)
    gid = tnum_get_attr_from_list(objs, "guid", None)
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

def tnum_date_as_token():
    dt = datetime.date.today().strftime("%Y-%m-%d %H-%M-%S")
    dt = dt.replace(" ", "_").replace(":", "-")
    return dt

###

import re
import json
import requests

def not_real_string(string):
    if re.search(r"[0-9,a-z%]+", string, re.IGNORECASE):
        return False
    else:
        return True

def tnum_make_tnum_json(subject="something", property="property", value=None,
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

def tnum_post_from_lists(subject, property, value=None, numeric_error=None, unit=None,
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
        tnum_make_tnum_json(sub, prop, val, err, u, t, no_empty_strings)
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

            tnum_var_posted_json = jsonnums
            args = {"numberspace": tnum_env["tnum_var.nspace"]}
            payload = f'{{"truenumbers":[{jsonnums}]}}'
            response = requests.post(
                f"http://{tnum_env['tnum_var.ip']}/v2/numberflow/numbers",
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

def tnum_post_statement(sentence, tags=None):
    if tags is None:
        tags = []
    
    payload = json.dumps({
        "noReturn": True,
        "skipStore": False,
        "truespeak": sentence,
        "tags": tags
    })
    
    args = {"numberspace": tnum_env["tnum_var.nspace"]}
    response = requests.post(
        f"http://{tnum_env['tnum_var.ip']}/v2/numberflow/numbers",
        json=payload,
        params=args,
        headers={"Accept": "application/json", "Content-Type": "application/json"}
    )

def tnum_post_objects(objects):
    if not isinstance(objects, (list, tuple)):
        objects = [objects]
    
    subject = [obj["subject"] for obj in objects]
    property = [obj["property"] for obj in objects]
    error = [obj.get("error") for obj in objects]
    unit = [obj.get("unit") for obj in objects]
    tags = [obj.get("tags", []) for obj in objects]
    
    tnum_post_from_lists(subject, property, objects, error, unit, tags)

def tnum_make_numeric_vector_string(numvec):
    if isinstance(numvec, list):
        nvec = numvec
    else:
        nvec = [0.0] * len(numvec)
    
    vvals = f"vector({','.join(str(n) for n in nvec)})"
    return vvals

def tnum_decode_numeric_vector_string(nvs):
    if nvs.startswith('"vector('):
        csl = nvs[8:-2]
        Nvec = [float(n) for n in csl.split(",")]
    else:
        Nvec = []
    
    return Nvec
