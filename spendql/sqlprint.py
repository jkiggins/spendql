import sqlite3

def printCursor(cursor, max_rows=-1):
    count = 0
    
    for row in cursor:
        if(max_rows >= 0 and count >= max_rows):
            break
        else:            
            print(row)
            count += 1

        
