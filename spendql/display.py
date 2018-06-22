def getTypes(aRow):
    typeSig = []
    for item in aRow:
        test = None
        # Try Parse Int
        try:
            test = int(item)
            typeSig.append(int)
            continue
        except ValueError:
            pass
        # Try Parse Float
        try:
            test = float(item)
            typeSig.append(float)
            continue
        except ValueError:
            pass

        typeSig.append(str)

    return typeSig


def getFormatStr(typeSig):
    fStrLookup = {int: "{: >20}", float: "{:4.2f >20}", str: "{: >20}"}

    return [fStrLookup[t] for t in typeSig]
    

def parseValues(aRow, typeSig):
    try:
        return [t(row) for row, t in zip(aRow, typeSig)]
    except:
        pass


def duplicateFirst(iter):
    first = next(iter)

    yield first
    yield first

    for item in iter:
        yield item


def printCursor(cursor):
    if cursor.description is not None:
        header = [des[0] for des in cursor.description]
        format_str = ' '.join(getFormatStr([str]*len(header)))
        header_print = format_str.format(*header)

        print(header_print)
        print("-"*len(header_print))

        rows = duplicateFirst(cursor)

        first = next(rows)
        
        typeSig = getTypes(first)
        formatStr = ' '.join(getFormatStr(typeSig))

        for row in rows:        
            print(formatStr.format(*parseValues(row, typeSig)))

        print("\n\n")