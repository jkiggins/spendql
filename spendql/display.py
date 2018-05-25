def printCursor(cursor):
    header = [des[0] for des in cursor.description]
    format_str = "{}" + "\t{}" * (len(header) - 1)
    header_print = format_str.format(*header)

    print(header_print)
    print('-'*len(header_print.replace('\t', ' '*5)))

    for row in cursor:        
        print(format_str.format(*row))

    print("\n\n")